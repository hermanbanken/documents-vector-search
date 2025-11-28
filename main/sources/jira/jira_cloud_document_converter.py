from langchain.text_splitter import RecursiveCharacterTextSplitter

class JiraCloudDocumentConverter:
    def __init__(self):
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=100,
        )

    def convert(self, document):
        return [{
            "id": document['key'],
            "url": self.__build_url(document),
            "modifiedTime": document['fields']['updated'],
            "createdTime": self.__get_created_time(document),
            "status": self.__get_status(document),
            "history": self.__get_history(document),
            "sprint": self.__get_sprint(document),
            "parentEpic": self.__get_parent_epic(document),
            "linkedTickets": self.__get_linked_tickets(document),
            "text": self.__build_document_text(document),
            "chunks": self.__split_to_chunks(document)
        }]
    
    def __build_document_text(self, document):
        main_info = self.__build_main_ticket_info(document)
        description_and_comments = self.__fetch_description_and_comments(document)

        return self.__convert_to_text([main_info, description_and_comments])

    def __split_to_chunks(self, document):
        chunks = [{
                "indexedData": self.__build_main_ticket_info(document)
            }]
        
        description_and_comments = self.__fetch_description_and_comments(document)
        if description_and_comments:
            for chunk in self.text_splitter.split_text(description_and_comments):
                chunks.append({
                    "indexedData": chunk
                })
        
            
        return chunks

    def __fetch_description_and_comments(self, document):
        description = self.__fetch_description(document)
        comments = [self.__convert_content_text(comment['body']) for comment in document['fields']['comment']['comments']]

        return self.__convert_to_text([description] + comments)

    def __fetch_description(self, document):
        description = document['fields']['description']
        if not description:
            return ""
        
        return self.__convert_content_text(description)

    def __convert_content_text(self, field_with_content):
        texts = []

        for content in field_with_content["content"]:
            if "content" in content:
                for content_of_content in content["content"]:
                    if "text" in content_of_content:
                        texts.append(content_of_content["text"])

        return self.__convert_to_text(texts, delimiter="\n")

    def __build_main_ticket_info(self, document):
        return f"{document['key']} : {document['fields']['summary']}"

    def __convert_to_text(self, elements, delimiter="\n\n"):
        return delimiter.join([element for element in elements if element]).strip()
    
    def __build_url(self, document):
        base_url = document['self'].split("/rest/api/")[0]
        return f"{base_url}/browse/{document['key']}"
    
    def __get_created_time(self, document):
        """Extract creation date from document fields."""
        try:
            return document['fields'].get('created')
        except (KeyError, AttributeError):
            return None
    
    def __get_status(self, document):
        """Extract status from document fields."""
        try:
            status = document['fields'].get('status')
            if status:
                # Status can be a dict with 'name' or just a string
                if isinstance(status, dict):
                    return status.get('name')
                return status
            return None
        except (KeyError, AttributeError):
            return None
    
    def __get_history(self, document):
        """Extract changelog history from document."""
        try:
            changelog = document.get('changelog')
            if changelog and 'histories' in changelog:
                return changelog['histories']
            return []
        except (KeyError, AttributeError):
            return []
    
    def __get_sprint(self, document):
        """Extract sprint information from document fields."""
        try:
            fields = document.get('fields', {})
            
            # Check common sprint field names
            # Sprint can be in fields.sprint or a custom field
            if 'sprint' in fields:
                sprint = fields['sprint']
                if sprint:
                    # Sprint can be a list or a single object
                    if isinstance(sprint, list):
                        return [self.__extract_sprint_info(s) for s in sprint if s]
                    return [self.__extract_sprint_info(sprint)]
            
            # Check custom fields for sprint (common patterns)
            for key, value in fields.items():
                if key.startswith('customfield_') and value:
                    # Sprint fields often contain 'sprint' in the name or have sprint-like structure
                    if isinstance(value, list) and len(value) > 0:
                        # Check if it looks like sprint data
                        first_item = value[0] if isinstance(value[0], dict) else value
                        if isinstance(first_item, dict) and ('name' in first_item or 'id' in first_item):
                            return [self.__extract_sprint_info(s) for s in value if s]
            
            return []
        except (KeyError, AttributeError):
            return []
    
    def __extract_sprint_info(self, sprint):
        """Extract relevant information from a sprint object."""
        if isinstance(sprint, dict):
            return {
                "id": sprint.get('id'),
                "name": sprint.get('name'),
                "state": sprint.get('state'),
                "startDate": sprint.get('startDate'),
                "endDate": sprint.get('endDate'),
                "completeDate": sprint.get('completeDate')
            }
        return {"name": str(sprint)} if sprint else None
    
    def __get_parent_epic(self, document):
        """Extract parent epic information from document fields."""
        try:
            fields = document.get('fields', {})
            
            # Check for parent field (for subtasks)
            if 'parent' in fields:
                parent = fields['parent']
                if parent and isinstance(parent, dict):
                    return {
                        "key": parent.get('key'),
                        "summary": parent.get('fields', {}).get('summary') if isinstance(parent.get('fields'), dict) else None
                    }
            
            # Check for epic link field
            if 'epicLink' in fields:
                epic_link = fields['epicLink']
                if epic_link:
                    return {"key": epic_link}
            
            # Check for epic link custom field (common pattern: customfield_*)
            for key, value in fields.items():
                if key.startswith('customfield_') and value:
                    # Epic link is usually a string (issue key) or could be an object
                    if isinstance(value, str) and value:
                        return {"key": value}
                    elif isinstance(value, dict) and value.get('key'):
                        return {"key": value.get('key')}
            
            return None
        except (KeyError, AttributeError):
            return None
    
    def __get_linked_tickets(self, document):
        """Extract linked tickets from document fields."""
        try:
            fields = document.get('fields', {})
            issue_links = fields.get('issuelinks', [])
            
            linked_tickets = []
            for link in issue_links:
                link_info = {}
                
                # Link can have 'inwardIssue' or 'outwardIssue'
                if 'inwardIssue' in link:
                    issue = link['inwardIssue']
                    link_info = {
                        "key": issue.get('key'),
                        "summary": issue.get('fields', {}).get('summary') if isinstance(issue.get('fields'), dict) else None,
                        "linkType": link.get('type', {}).get('inward', '') if isinstance(link.get('type'), dict) else None,
                        "direction": "inward"
                    }
                elif 'outwardIssue' in link:
                    issue = link['outwardIssue']
                    link_info = {
                        "key": issue.get('key'),
                        "summary": issue.get('fields', {}).get('summary') if isinstance(issue.get('fields'), dict) else None,
                        "linkType": link.get('type', {}).get('outward', '') if isinstance(link.get('type'), dict) else None,
                        "direction": "outward"
                    }
                
                if link_info.get('key'):
                    linked_tickets.append(link_info)
            
            return linked_tickets
        except (KeyError, AttributeError):
            return [] 