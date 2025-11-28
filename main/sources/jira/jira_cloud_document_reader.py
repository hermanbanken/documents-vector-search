import requests
import logging

from ...utils.retry import execute_with_retry

class JiraCloudDocumentReader:
    def __init__(self, 
                 base_url, 
                 query,
                 email=None,
                 api_token=None,
                 batch_size=500,
                 number_of_retries=3,
                 retry_delay=1,
                 max_skipped_items_in_row=5):
        # "email" and "api_token" must be provided for Cloud
        if not email or not api_token:
            raise ValueError("Both 'email' and 'api_token' must be provided for Jira Cloud.")

        # Ensure base_url has the correct Cloud format
        if not base_url.endswith('.atlassian.net'):
            raise ValueError("Base URL must be a Jira Cloud URL (ending with .atlassian.net)")
        
        self.base_url = base_url
        self.query = query
        self.email = email
        self.api_token = api_token
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
        # The new API uses token-based pagination with nextPageToken
        # We need to implement custom pagination instead of using the batch utility
        next_page_token = None
        skipped_items_in_row = 0
        
        while True:
            try:
                params = {
                    'jql': self.query,
                    "maxResults": self.batch_size,
                    "fields": self.fields,
                    "expand": self.expand,
                }
                
                if next_page_token:
                    params['nextPageToken'] = next_page_token
                
                search_result = self.__request_items(params)
                
                issues = search_result.get('issues', [])
                if not issues:
                    break
                
                skipped_items_in_row = 0
                
                for issue in issues:
                    yield issue
                
                # Check if there's a next page
                next_page_token = search_result.get('nextPageToken')
                if not next_page_token:
                    break
                    
            except Exception as e:
                if skipped_items_in_row >= self.max_skipped_items_in_row:
                    logging.error(f"Max number of skipped items in row ({self.max_skipped_items_in_row}) was reached. Stopping reading.")
                    raise e
                
                logging.warning(f"Skipping batch because of an error: {e}")
                skipped_items_in_row += 1
                # Try to continue with next page if we have a token
                if not next_page_token:
                    break

    def __request_items(self, params):
        def do_request():
            # Use POST /rest/api/3/search/jql endpoint (required migration)
            # The old /rest/api/3/search endpoint is deprecated
            url = self.__add_url_prefix('/rest/api/3/search/jql')
            
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
            
            response = requests.post(url=url,
                                    headers={
                                        "Accept": "application/json",
                                        "Content-Type": "application/json"
                                    }, 
                                    json=json_body,
                                    auth=(self.email, self.api_token))
            
            # Log error response details before raising
            if not response.ok:
                error_details = {
                    "status_code": response.status_code,
                    "url": response.url,
                    "request_body": json_body
                }
                try:
                    error_details["response_body"] = response.json()
                except (ValueError, requests.exceptions.JSONDecodeError):
                    error_details["response_text"] = response.text
                
                logging.error(f"JIRA Cloud API error: {error_details}")
            
            response.raise_for_status()
            return response.json()

        return execute_with_retry(do_request, f"Requesting items with params: {params}", self.number_of_retries, self.retry_delay) 