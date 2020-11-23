from apiclient.discovery import build
import config
api_key = config.api_key # change to your api key


class GoogleSearch:
    def __init__(self):
        self.resource = build("customsearch", 'v1', developerKey=api_key).cse()

    def search(self, query: str):
        result = self.resource.list(q=query, cx='a9e157b696438b278').execute()
        first_item = result['items'][0]
        title = first_item['title']
        snippet = first_item['snippet']
        return title, snippet

# kind', 'title', 'htmlTitle', 'link', 'displayLink', 'snippet', 'htmlSnippet', 'cacheId', 'formattedUrl',
# 'htmlFormattedUrl', 'pagemap'
