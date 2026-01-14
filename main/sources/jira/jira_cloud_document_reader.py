import subprocess
import json
import logging

from ...utils.retry import execute_with_retry

class JiraCloudDocumentReader:
    def __init__(self, 
                 base_url, 
                 query,
                 batch_size=500,
                 number_of_retries=3,
                 retry_delay=1,
                 max_skipped_items_in_row=5):
        # Ensure base_url has the correct Cloud format
        if not base_url.endswith('.atlassian.net'):
            raise ValueError("Base URL must be a Jira Cloud URL (ending with .atlassian.net)")
        
        self.base_url = base_url
        self.query = query
        self.batch_size = batch_size
        self.number_of_retries = number_of_retries
        self.retry_delay = retry_delay
        self.max_skipped_items_in_row = max_skipped_items_in_row
        self.fields = "summary,description,comment,updated,status,created,sprint,parent,epicLink,issuelinks"
        self.expand = "changelog"

    def read_all_documents(self):
        return self.__read_items()

    def get_number_of_documents(self):
        # The new API doesn't return a total count
        # We'll need to count all items by iterating through them
        # For now, return None to indicate we can't determine the total upfront
        # The caller should handle this appropriately
        logging.warning("JIRA Cloud API v3 doesn't provide total count. Returning None.")
        return None

    def get_reader_details(self) -> dict:
        return {
            "type": "jiraCloud",
            "baseUrl": self.base_url,
            "query": self.query,
            "batchSize": self.batch_size,
            "fields": self.fields,
            "expand": self.expand,
        }

    def __add_url_prefix(self, relative_path):
        return self.base_url + relative_path

    def __read_items(self):
        # acli doesn't support offset pagination, so we'll fetch in batches
        # by using limit and tracking which issues we've seen
        # We'll use a sliding window approach with JQL date filters if needed
        skipped_items_in_row = 0
        all_issues = []
        batch_start = 0
        
        while True:
            try:
                params = {
                    'jql': self.query,
                    "maxResults": self.batch_size,
                    "fields": self.fields,
                    "expand": self.expand,
                }
                
                search_result = self.__request_items(params)
                
                # acli returns an array directly
                issues = search_result if isinstance(search_result, list) else []
                
                if not issues:
                    break
                
                skipped_items_in_row = 0
                
                # Fetch full details for each issue since search doesn't return all fields
                for issue in issues:
                    issue_key = issue.get('key')
                    if issue_key:
                        try:
                            full_issue = self.__fetch_full_issue(issue_key)
                            yield full_issue
                        except Exception as e:
                            logging.warning(f"Failed to fetch full details for {issue_key}: {e}")
                            # Yield the partial issue as fallback
                            yield issue
                    else:
                        yield issue
                
                # If we got fewer results than requested, we've reached the end
                if len(issues) < self.batch_size:
                    break
                
                # For next batch, we need a different approach since acli doesn't support offset
                # We'll use the --paginate flag to get all results at once
                # But for now, let's just process what we can get
                # Note: This is a limitation - we'll only get one batch
                # To get all results, we'd need to use --paginate or fetch by keys
                logging.warning(f"acli search doesn't support offset pagination. Fetched {len(issues)} issues. Use --paginate for all results.")
                break
                    
            except Exception as e:
                if skipped_items_in_row >= self.max_skipped_items_in_row:
                    logging.error(f"Max number of skipped items in row ({self.max_skipped_items_in_row}) was reached. Stopping reading.")
                    raise e
                
                logging.warning(f"Skipping batch because of an error: {e}")
                skipped_items_in_row += 1
                break

    def __request_items(self, params):
        def do_request():
            # Use acli to execute Jira API calls
            # acli handles authentication automatically
            
            # Convert params to JSON body format
            # Fields should be an array for the new API
            fields_param = params.get('fields', self.fields)
            if isinstance(fields_param, str):
                fields_list = [f.strip() for f in fields_param.split(',')]
            else:
                fields_list = fields_param
            
            expand_param = params.get('expand', self.expand)
            # Keep expand as string (comma-separated) for the /search/jql endpoint
            if isinstance(expand_param, list):
                expand_str = ','.join(expand_param)
            else:
                expand_str = expand_param
            
            # Build JSON body for /rest/api/3/search/jql endpoint
            # Note: This endpoint uses token-based pagination, not startAt
            json_body = {
                "jql": params.get('jql', self.query),
                "maxResults": params.get('maxResults', self.batch_size),
                "fields": fields_list
            }
            
            # Add nextPageToken if provided (for pagination)
            if 'nextPageToken' in params:
                json_body["nextPageToken"] = params['nextPageToken']
            
            # Only add expand if it's provided and not empty
            if expand_str:
                json_body["expand"] = expand_str
            
            # Use acli to make the API call
            # acli jira workitem search --jql "<jql>" --fields <fields> --paginate --json
            # Note: acli search returns an array of issues directly, not wrapped in an object
            # We use --paginate to get all results matching the query
            acli_args = [
                'acli',
                'jira',
                'workitem',
                'search',
                '--jql', params.get('jql', self.query),
                '--paginate',  # Fetch all results
                '--json'
            ]
            
            # acli search has limited field support - only basic fields work
            # We'll use default fields and fetch full details per issue if needed
            # For now, use only supported fields: summary, description, status, assignee, priority, issuetype, key
            supported_fields = ['summary', 'description', 'status', 'assignee', 'priority', 'issuetype', 'key']
            acli_args.extend(['--fields', ','.join(supported_fields)])
            
            # Note: We'll need to fetch individual issues for comment, changelog, sprint, etc.
            # This is a limitation of acli search - it doesn't support all fields
            
            # Note: acli doesn't support expand parameter directly
            # Changelog and other expand fields may not be available via search
            # We may need to fetch individual issues for full changelog data if needed
            
            try:
                result = subprocess.run(
                    acli_args,
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                response_data = json.loads(result.stdout)
                
                # acli returns an array directly, but our code expects an object with 'issues' key
                # For compatibility, we'll wrap it
                if isinstance(response_data, list):
                    return response_data  # Return as-is, __read_items will handle it
                else:
                    return response_data
                
            except subprocess.CalledProcessError as e:
                error_details = {
                    "returncode": e.returncode,
                    "stdout": e.stdout,
                    "stderr": e.stderr,
                    "command": ' '.join(acli_args)
                }
                logging.error(f"JIRA Cloud API error via acli: {error_details}")
                raise Exception(f"acli command failed: {e.stderr}")
            except json.JSONDecodeError as e:
                error_details = {
                    "stdout": result.stdout if 'result' in locals() else None,
                    "json_error": str(e)
                }
                logging.error(f"Failed to parse acli JSON response: {error_details}")
                raise Exception(f"Failed to parse acli response: {e}")

        return execute_with_retry(do_request, f"Requesting items with params: {params}", self.number_of_retries, self.retry_delay)
    
    def __fetch_full_issue(self, issue_key):
        """Fetch full issue details using acli view command"""
        def do_request():
            acli_args = [
                'acli',
                'jira',
                'workitem',
                'view',
                issue_key,
                '--fields', '*all',  # Get all fields
                '--json'
            ]
            
            try:
                result = subprocess.run(
                    acli_args,
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                response_data = json.loads(result.stdout)
                # acli view returns a single issue object (or array with one item)
                if isinstance(response_data, list):
                    return response_data[0] if len(response_data) > 0 else response_data
                return response_data
                
            except subprocess.CalledProcessError as e:
                error_details = {
                    "returncode": e.returncode,
                    "stdout": e.stdout,
                    "stderr": e.stderr,
                    "command": ' '.join(acli_args)
                }
                logging.error(f"Failed to fetch full issue {issue_key} via acli: {error_details}")
                raise Exception(f"acli view command failed: {e.stderr}")
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse acli view response for {issue_key}: {e}")
                raise Exception(f"Failed to parse acli view response: {e}")
        
        return execute_with_retry(do_request, f"Fetching full issue {issue_key}", self.number_of_retries, self.retry_delay) 