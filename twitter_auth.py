import json
import pandas as pd
import matplotlib.pyplot as plt
import requests
import twitter

consumer_key = "QwPdI6n4HLlHTxUc402qBK4R9"
consumer_secret = "WxneFp5WqRCIZylQwkRNgv9egTe8C0COHXNvbO3EzVFCp4u212"
access_token = "959387419199131649-RppQC0tXAIDYXeH9oeokJ8QU17Nw6tZ"
access_secret = 'sup758H9a7E6GxTfbzCxTYvAt9SmdqNKZiVKGN208U32m'

api = twitter.Api(consumer_key=consumer_key,
                  consumer_secret=consumer_secret,
                  access_token_key=access_token,
                  access_token_secret=access_secret)

friends = api.GetFriends()

#friends>value(x)>screen_name
for x in friends:
    print(x.screen_name)

""""
api.PostDirectMessage(user, text)
api.GetUser(user)
api.GetReplies()
api.GetUserTimeline(user)
api.GetHomeTimeline()
api.GetStatus(status_id) #put id of the post
api.DestroyStatus(status_id)
api.GetFeatured()
api.GetDirectMessages()
api.GetSentDirectMessages()
api.PostDirectMessage(user, text) #posts a private message
api.PostUpdates(status='test api') #tweets a public message --- PostUpdates[0].id > returns id of the post
api.DestroyDirectMessage(message_id)
api.DestroyFriendship(user)
api.CreateFriendship(user)
api.LookupFriendship(user)
api.VerifyCredentials()"""