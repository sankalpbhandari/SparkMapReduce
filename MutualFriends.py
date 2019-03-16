from pyspark import SparkContext


def friendmap(value):
    value = value.split("	")
    keys = []
    if len(value) == 2 and value[1].strip() != "":
        user = int(value[0])
        friends = value[1].split(",")
        for friend in friends:
            friend = int(friend)
            if user == friend:
                continue
            if int(friend) < int(user):
                user_key = (friend, user)
            else:
                user_key = (user, friend)
            keys.append((user_key, value[1].split(',')))
    return keys


def friendreduce(key, value):
    reducer = []
    for friend in key:
        if friend in value:
            reducer.append(int(friend))
    return len(reducer)


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    frnds_l = sc.textFile("dataset/NetworkData.txt", 1)
    frnd_l = frnds_l.flatMap(friendmap)
    Commonfriends = frnd_l.reduceByKey(friendreduce).filter(lambda x: x[1] != 0)
    Commonfriends.saveAsTextFile("Output/MutualFriendsOutput")
    sc.stop()
