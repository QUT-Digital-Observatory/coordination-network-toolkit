import csv
import random
import datetime as dt


header = [
    "message_id",
    "user_id",
    "username",
    "repost_id",
    "reply_id",
    "message",
    "timestamp",
    "urls"
]


words = [
    "apple",
    "pear",
    "banana",
    "pineapple",
    "mango",
    "cat",
    "dog",
    "squirrel",
    "reindeer",
    "wombat",
    "camel",
    "notebook",
    "pen",
    "pencil",
    "graph"
]


def generate_sample_data(file_path, num_messages=100, num_users=10, num_urls=10, example_domain="http://example.com/"):
    users = list()
    for i in range(0, num_users):
        users.append((i, "user_" + str(i)))

    urls = list()
    for i in range(0, num_urls):
        urls.append(example_domain + str(i))

    message_ids = list()
    with open(file_path, 'w') as fh:
        writer = csv.writer(fh)

        writer.writerow(header)

        for message_id in range(1, num_messages + 1):
            msg = [message_id]

            # user_id and username
            msg.extend(random.choice(users))

            # repost and reply ids
            # Areas for possible improvement:
            # - configurable probabilities
            # - possibility of values outside existing data
            # - should it be allowed to have both repost_id and reply_id?
            if len(message_ids) == 0:
                repost_id = None
                reply_id = None
            else:
                repost_id = random.choice(message_ids) if random.random() <= 0.3 else None
                reply_id = None
                if not repost_id and random.random() <= 0.3:
                    reply_id = random.choice(message_ids)
            msg.extend([repost_id, reply_id])

            # message
            # Areas for possible improvement:
            # - how to manage likelihood of repeat messages? Same as message_ids?
            message_text = " ".join(random.sample(words, random.randint(1, 4)))
            msg.append(message_text)

            # timestamp
            # They'll *all* be within the threshold like this
            msg.append(dt.datetime.now().timestamp())

            # urls
            # Areas for possible improvement:
            # - probably should be more likely that no URLs are included
            msg.append(random.sample(urls, random.randint(0, 2)))

            writer.writerow(msg)
            message_ids.append(message_id)


if __name__ == '__main__':
    generate_sample_data("sample_data.csv")
