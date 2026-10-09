'''
Replaces content that was published on the user's page with the string "user"
'''

import pandas as pd

CONTENT_FILE = "../../in/content.csv"
CONTENT_FILE_OUT = "../../out/content.csv"

content = pd.read_csv(CONTENT_FILE)

content['subreddit'] = content['subreddit'].apply(lambda x: 'user' if str(x).startswith('u/') else x)

content.to_csv(CONTENT_FILE_OUT, index=False)
