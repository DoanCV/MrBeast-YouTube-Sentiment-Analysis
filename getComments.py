# Import libraries
import json
from csv import writer
from apiclient.discovery import build
import pandas as pd
import pickle
import urllib.request
import urllib

# API key from YouTube Data API key
key = <'Key'>
videoId = 'PKtnafFtfEo' # Youtube Rewind 2020, Thank God It's Over
channelId = 'UCX6OQ3DkcsbYNE6H8uQQuVA' # MrBeast 

def build_service():
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"
    return build(YOUTUBE_API_SERVICE_NAME,
                 YOUTUBE_API_VERSION,
                 developerKey=key)

def get_comments(part='snippet', 
                 maxResults=100, 
                 textFormat='plainText',
                 order='time',
                 allThreadsRelatedToChannelId=channelId,
                 csv_filename="MrBeast_comments"
                 ):
  
  # Initialize empty lists which will store the stats on a video
  comments, commentsId, authorurls, authornames, repliesCount, likesCount, viewerRating, dates, vidIds, totalReplyCounts,vidTitles = [], [], [], [], [], [], [], [], [], [], []

  # Build the serivce
  service = build_service()

  # Call the API with the service
  response = service.commentThreads().list(
        part=part,
        maxResults=maxResults,
        textFormat='plainText',
        order=order,
        allThreadsRelatedToChannelId=channelId
  ).execute()

  # There is a limit to the amount of data I can pull. 
  # For MrBeast, his channel is quite large and active so this loop will stop running at the quota
  # Typically I would set a hard cap on the number of comments to pull that is lower than the quota to test if the function is working properly but I have a guide on this API sourced below.

  while response:
    for item in response['items']:
            # Index item for desired data features
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            comment_id = item['snippet']['topLevelComment']['id']
            reply_count = item['snippet']['totalReplyCount']
            like_count = item['snippet']['topLevelComment']['snippet']['likeCount']
            authorurl = item['snippet']['topLevelComment']['snippet']['authorChannelUrl']
            authorname = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
            date = item['snippet']['topLevelComment']['snippet']['publishedAt']
            vidId = item['snippet']['topLevelComment']['snippet']['videoId']
            totalReplyCount = item['snippet']['totalReplyCount']
            vidTitle = get_vid_title(vidId)

            # Append to lists
            comments.append(comment)
            commentsId.append(comment_id)
            repliesCount.append(reply_count)
            likesCount.append(like_count)
            authorurls.append(authorurl)
            authornames.append(authorname)
            dates.append(date)
            vidIds.append(vidId)
            totalReplyCounts.append(totalReplyCount)
            vidTitles.append(vidTitle)

    try:
            if 'nextPageToken' in response:
                response = service.commentThreads().list(
                    part=part,
                    maxResults=maxResults,
                    textFormat=textFormat,
                    order=order,
                    allThreadsRelatedToChannelId=channelId,
                    pageToken=response['nextPageToken']
                ).execute()
            else:
                break
    except: break
  
  return {
        'comment': comments,
        'comment_id': commentsId,
        'author_url': authorurls,
        'author_name': authornames,
        'reply_count' : repliesCount,
        'like_count' : likesCount,
        'date': dates,
        'vidid': vidIds,
        'total_reply_counts': totalReplyCounts,
        'vid_title': vidTitles
    }

def get_vid_title(vidid):
    # VideoID = "LAUa5RDUvO4"
    # Usually, the videoID is after the ?v=. This information can also be accessed when clicking share on the video itself.
    params = {"format": "json", "url": "https://www.youtube.com/watch?v=%s" % vidid}
    url = "https://www.youtube.com/oembed"
    query_string = urllib.parse.urlencode(params)
    url = url + "?" + query_string

    with urllib.request.urlopen(url) as response:
        response_text = response.read()
        data = json.loads(response_text.decode())
        return data['title']

if __name__ == '__main__':
    MrBeast_comments = get_comments()
    df = pd.DataFrame(MrBeast_comments)
    print(df.shape)
    print(df.head())
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['just_date'] = df['date'].dt.date
    df.to_csv('./MrBeast_comments.csv')