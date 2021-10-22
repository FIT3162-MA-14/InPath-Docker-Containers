from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from time import sleep
import csv

"""
Learning metrics in student_dict
0: Number of words
1: Number of messages read
2: Total time spent
3: Number of posts
4: Average number of posts (Total post / number of category)
5: Number of replies
6: Likes received
7: Is moderator? 1:0
"""

# Producer send merged topics
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

consumer = KafkaConsumer(
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='money',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)
consumer.subscribe(["pg.public.users",
                   "pg.public.user_stats", "pg.public.posts"])


def extract_var(topic: int, message):
    src = message["after"]
    # users table: id, username, moderator
    if topic == 0:
        uid = src["id"]
        uname = src["username"]
        is_mod = src["moderator"]
        return (uid, uname, is_mod)
    # posts: user_id, word_count, post_number
    else:
        uid = src["user_id"]
        words = src["word_count"]
        pn = src["post_number"]
        return (uid, words, pn)


def extract_var2(message):
    # msg = loads(message)
    src = message["after"]
    # users_stats: user_id, days_visited, posts_read_count, likes_received
    uid = src["user_id"]
    days = src["days_visited"]
    read = src["posts_read_count"]
    like = src["liked_received"]
    post = src["post_count"]
    topic = src["topic_count"]
    return (uid, days, read, like, post, topic)


students_dict = {}

uid, uname, is_mod, days, read, like, post, topic, words, post_number = 0, "", False, 0, 0, 0, 0, 0, 0, 0
for event in consumer:
    student = event.value
    if student["source"]["table"] == "users":
        uid, uname, is_mod = extract_var(0, student)
        if not bool(is_mod):
            # Create a new user
            createUser(uid)
            lst = [uid] + students_dict[uid][:-1]
            producer.send('dataset', value=[int(x) for x in lst])
    elif student["source"]["table"] == "user_stats":
        uid, days, read, like, post, topic = extract_var2(student)
        if not bool(is_mod):
            if uid not in students_dict:
                createUser(uid)
            data = students_dict[uid]

            data[2], data[1], data[6] = days, read, like
            lst = [uid] + students_dict[uid][:-1]
            producer.send('dataset', value=[int(x) for x in lst])
    elif student["source"]["table"] == "posts":
        uid, words, post_number = extract_var(1, student)
        if not bool(is_mod):
            if uid not in students_dict:
                createUser(uid)
            data = students_dict[uid]
            data[0] += words

            # When pn = 0, it is the first post for topic creation
            if post_number == 0:
                data[3] += 1
                data[4] = data[3] / 7
            lst = [uid] + students_dict[uid][:-1]
            producer.send('dataset', value=[int(x) for x in lst])

### Create a new user in dictionary
def createUser(uid):
    students_dict[uid] = [0]* 9

### Convert to json from dict

def convertDict():
    jsonDataList = []
    
    for uid in students_dict:
        new_dict = {
            "uid": uid,
            "numOfWords": students_dict[uid][0],
            "messageRead": students_dict[uid][1],
            "timeSpent":  students_dict[uid][2],
            "numOfPosts":  students_dict[uid][3],
            "avgPostPerCat":  students_dict[uid][4],
            "numOfReplies":  students_dict[uid][5],
            "likeReceived":  students_dict[uid][6],
        }
        jsonData = json.dumps(new_dict)
        jsonDataList.append(jsonData)

    producer.send('dataset', value=jsonDataList)
