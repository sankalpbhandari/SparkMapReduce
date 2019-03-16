from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    business_l = sc.textFile("dataset/business.csv").distinct(1).map(lambda x: x.split("::"))
    review_l = sc.textFile("dataset/review.csv").distinct(1).map(lambda x: x.split("::"))

    filtered_business = business_l.filter(lambda record: "Colleges & Universities" in record[2]).map(
        lambda record: (record[0], ""))
    review_map = review_l.map(lambda record: (record[2], (record[1], record[3])))
    result = review_map.join(filtered_business, 1).map(lambda x: x[1][0])
    result.saveAsTextFile("Output/UserReviewsOutput")
    sc.stop()
