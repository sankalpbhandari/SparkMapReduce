from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    business_l = sc.textFile("dataset/business.csv").map(lambda x: x.split("::")).filter(lambda x: "NY" in x[1])
    review_l = sc.textFile("dataset/review.csv").map(lambda x: x.split("::"))
    business_set = business_l.map(lambda x: (x[0], x[1] + "  " + x[2]))

    sum_rating = review_l.map(lambda a: (a[2], float(a[3]))).reduceByKey(lambda a, b: a + b).partitionBy(1)
    count_rating = review_l.map(lambda a: (a[2], 1)).reduceByKey(lambda a, b: a + b).partitionBy(1)

    join_result = sum_rating.join(count_rating)
    rating_avg = join_result.map(lambda a: (a[0], a[1][0] / a[1][1]))

    result_final = business_set.join(rating_avg).distinct(1).sortBy(lambda a: a[1][1], ascending=False)
    result_final.saveAsTextFile("Output/TopTenBusinessOutput")
    print(result_final.top(10))
    sc.stop()
