import subprocess
import json
import urllib.parse
import logging

from ...utils.retry import execute_with_retry
from ...utils.batch import read_items_in_batches

class ConfluenceCloudDocumentReader:
    def __init__(self, 
                 base_url, 
                 query,
                 batch_size=50, 
                 number_of_retries=3, 
                 retry_delay=1, 
                 max_skipped_items_in_row=5,
                 read_all_comments=False):
        # Ensure base_url has the correct Cloud format
        if not base_url.endswith('.atlassian.net'):
            raise ValueError("Base URL must be a Confluence Cloud URL (ending with .atlassian.net)")
        
        self.base_url = base_url
        self.query = ConfluenceCloudDocumentReader.build_page_query(query)
        self.batch_size = batch_size
        # Confluence has hierarchical comments, we can read first level by adding "children.comment.body.storage" to "expand" parameter
        # but to read all comments we need to make additional request with "depth=all" parameter
        self.expand = "content.body.storage,content.ancestors,content.version,content.children.comment" if read_all_comments else "content.body.storage,content.ancestors,content.version,content.children.comment.body.storage"
        self.number_of_retries = number_of_retries
        self.retry_delay = retry_delay
        self.max_skipped_items_in_row = max_skipped_items_in_row
        self.read_all_comments = read_all_comments
    
    def read_all_documents(self):
        for page in self.__read_items():
            yield {
                "page": page,
                "comments": self.__read_comments(page)
            }

    def get_number_of_documents(self):
        search_result = self.__request(
            self.__add_url_prefix('/wiki/rest/api/search'),
            {
                "cql": self.query,
                "limit": 1,
                "start": 0
            })
        
        return search_result['totalSize']
    
    def get_reader_details(self) -> dict:
        return {
            "type": "confluenceCloud",
            "baseUrl": self.base_url,
            "query": self.query,
            "expand": self.expand,
            "batchSize": self.batch_size,
            "readAllComments": self.read_all_comments,
        }
    
    @staticmethod
    def build_page_query(user_query):
        if not user_query:
            return "type=page"

        return f'type=page AND ({user_query})'

    def __add_url_prefix(self, relative_path):
        return self.base_url + relative_path
    
    def __read_comments(self, page):
        if page['content']['children']['comment']['size'] == 0:
            return []

        if not self.read_all_comments:
            return page['content']['children']['comment']['results']

        read_batch_func = lambda start_at, batch_size, cursor = None: self.__request(
            self.__add_url_prefix(f"/wiki/rest/api/content/{page['content']['id']}/child/comment"),
            {
                "limit": batch_size,
                "start": start_at,
                "expand": "body.storage",
                "depth": "all"
            })

        comments_generator = read_items_in_batches(read_batch_func,
                              fetch_items_from_result_func=lambda result: result['results'],
                              fetch_total_from_result_func=lambda result: result['size'],
                              batch_size=self.batch_size,
                              max_skipped_items_in_row=self.max_skipped_items_in_row,
                              itemsName="comments")

        return [comment for comment in comments_generator]

    def __read_items(self):
        read_batch_func = lambda start_at, batch_size, cursor: self.__request(
            self.__add_url_prefix('/wiki/rest/api/search'),
            {
                "cql": self.query,
                "limit": batch_size,
                "start": start_at,
                "expand": self.expand,
                "cursor": cursor
            })

        return read_items_in_batches(read_batch_func,
                              fetch_items_from_result_func=lambda result: result['results'],
                              fetch_total_from_result_func=lambda result: result['totalSize'],
                              batch_size=self.batch_size,
                              max_skipped_items_in_row=self.max_skipped_items_in_row,
                              itemsName="pages",
                              cursor_parser=ConfluenceCloudDocumentReader.__parse_cursor)

    def __request(self, url, params):
        def do_request():
            # Use acli to execute Confluence API calls
            # acli handles authentication automatically
            
            # Extract domain from base_url (e.g., "your-domain" from "https://your-domain.atlassian.net")
            domain = self.base_url.replace('https://', '').replace('.atlassian.net', '')
            
            # Determine the API endpoint from the URL
            if '/wiki/rest/api/search' in url:
                # Search API
                acli_args = [
                    'acli',
                    'confluence',
                    'search',
                    '--site', domain,
                    '--cql', params.get('cql', self.query),
                    '--limit', str(params.get('limit', self.batch_size)),
                    '--start', str(params.get('start', 0)),
                    '--output', 'json'
                ]
                
                if 'expand' in params:
                    acli_args.extend(['--expand', params['expand']])
                
                if 'cursor' in params and params['cursor']:
                    acli_args.extend(['--cursor', params['cursor']])
                    
            elif '/wiki/rest/api/content/' in url and '/child/comment' in url:
                # Comments API
                content_id = url.split('/wiki/rest/api/content/')[1].split('/child/comment')[0]
                acli_args = [
                    'acli',
                    'confluence',
                    'content',
                    'comments',
                    '--site', domain,
                    '--content-id', content_id,
                    '--limit', str(params.get('limit', self.batch_size)),
                    '--start', str(params.get('start', 0)),
                    '--output', 'json'
                ]
                
                if 'expand' in params:
                    acli_args.extend(['--expand', params['expand']])
                
                if 'depth' in params:
                    acli_args.extend(['--depth', params['depth']])
            else:
                # Generic content API - try to use acli confluence content
                # Fallback to a more generic approach
                raise ValueError(f"Unsupported URL pattern for acli: {url}")
            
            try:
                result = subprocess.run(
                    acli_args,
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                response_data = json.loads(result.stdout)
                return response_data
                
            except subprocess.CalledProcessError as e:
                error_details = {
                    "returncode": e.returncode,
                    "stdout": e.stdout,
                    "stderr": e.stderr,
                    "command": ' '.join(acli_args)
                }
                logging.error(f"Confluence Cloud API error via acli: {error_details}")
                raise Exception(f"acli command failed: {e.stderr}")
            except json.JSONDecodeError as e:
                error_details = {
                    "stdout": result.stdout if 'result' in locals() else None,
                    "json_error": str(e)
                }
                logging.error(f"Failed to parse acli JSON response: {error_details}")
                raise Exception(f"Failed to parse acli response: {e}")

        return execute_with_retry(do_request, f"Requesting items with params: {params}", self.number_of_retries, self.retry_delay)
    
    @staticmethod
    def __parse_cursor(result):
        if '_links' not in result:
            raise ValueError(f"No '_links' in the result: {result}")
        
        if 'next' not in result['_links']:
            return None

        next_link = result['_links']['next']
        url_params = ConfluenceCloudDocumentReader.parse_url_params(next_link)
        
        if 'cursor' not in url_params:
            raise ValueError(f"No 'cursor' parameter found in the next link: {next_link}")
        
        return url_params['cursor'][0]
    
    @staticmethod
    def parse_url_params(url):
        parsed = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed.query)
        return {key: values for key, values in query_params.items()} if query_params else {}