from pyspark import SparkContext


def friendmap(value):
    keys = []
    user = int(value[0])
    if value[1][0] != [""]:
        friends = [int(x) for x in value[1][0]]
        for friend in friends:
            friend = int(friend)
            if user == friend:
                continue
            if int(friend) < int(user):
                user_key = (friend, user)
            else:
                user_key = (user, friend)
            keys.append((user_key, [friends, value[1][1]]))
    return keys


def friendreduce(key, value):
    reducer = []
    for friend in key[0]:
        if friend in value[0]:
            reducer.append(int(friend))
    return [len(reducer), value[1], key[1]]


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    frnds_l = sc.textFile("dataset/NetworkData.txt", 1).map(lambda frnd: frnd.split("	")).filter(
        lambda frnd: len(frnd) == 2).map(lambda frnd: (frnd[0], frnd[1].split(",")))
    userdata_l = sc.textFile("dataset/UserData.txt", 1).map(lambda data: data.split(",")).map(lambda data: (
        data[0],
        data[1] + " " + data[2] + ", " + data[3] + ", " + data[4] + ", " + data[5] + ", " + data[7] + "-" + data[6]))
    res_l = frnds_l.join(userdata_l).coalesce(1)
    frnd_l = res_l.flatMap(friendmap)
    Commonfriends = frnd_l.reduceByKey(friendreduce).filter(lambda x: x[1][0] != 0).map(lambda a: (a[1], a[0]))
    top_ten = Commonfriends.sortBy(lambda a: a[0][0], ascending=False)
    top_ten.saveAsTextFile("Output/TopTenMutualFriendsOutput")
    print(top_ten.top(10))
    sc.stop()
