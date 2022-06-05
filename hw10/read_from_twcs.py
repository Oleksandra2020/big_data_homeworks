from colorsys import TWO_THIRD
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.types import *
from pyspark.sql.functions import col
from operator import itemgetter
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('SimpleSparkProject').getOrCreate()


def get_categories():
    f = open('CA_category_id.json')
    data = json.load(f)
    dct = {}
    for i in data['items']:
        id = i['id']
        name = i['snippet']['title']
        dct[id] = name
    return dct


def get_dates(df):
    d = df.select("trending_date").collect()
    dates = []
    seen = set()
    for i in d:
        if i[0] not in seen:
            dates.append(i[0])
            seen.add(i[0])
    return dates


def first_q(df):
    grouped_df = df.groupBy('video_id').count().sort(desc("count")).limit(10)
    video_ids = grouped_df.select("video_id").collect()

    schema = StructType([
        StructField("id", StringType()),
        StructField("title", StringType()),
        StructField("description", StringType()),
        StructField("latest_views", LongType()),
        StructField("latest_likes", LongType()),
        StructField("latest_dislikes", LongType()),
        StructField("trending_days", ArrayType(
            StructType([
                StructField("date", StringType()),
                StructField("views", LongType()),
                StructField("likes", LongType()),
                StructField("dislikes", LongType()),
            ])
        )),
    ])

    data = []

    for i in range(10):
        rows = df.filter(df.video_id == video_ids[i][0]).collect()
        id = rows[0]['video_id']
        title = rows[0]['title']
        description = rows[0]['description']
        views = int(rows[-1]['views'])
        likes = int(rows[-1]['likes'])
        dislikes = int(rows[-1]['dislikes'])
        days = []
        for d in rows:
            days.append([d['trending_date'], int(d['views']),
                         int(d['likes']), int(d['dislikes'])])
        data.append([id, title, description, views, likes, dislikes, days])

    res_df = spark.createDataFrame(data=data, schema=schema)
    res_df.printSchema()
    # res_df.show(truncate=False)

    res_df.write.mode("Overwrite").json("./first_q_schema")


def second_q(df):
    schema = StructType([
        StructField("start_date", StringType()),
        StructField("end_date", StringType()),
        StructField("category_id", StringType()),
        StructField("category_name", StringType()),
        StructField("number_of_videos", LongType()),
        StructField("total_views", LongType()),
        StructField("video_ids", ArrayType(StringType())),
    ])

    data = []
    dates = get_dates(df)
    categories = get_categories()

    week_len = 7
    for i in range(0, len(dates), week_len):
        if i+week_len > len(dates) - 1:
            week = dates[i:len(dates) - 1]
        else:
            week = dates[i:i+week_len]
        this_week_videos = df.withColumn("views", col("views").cast("int")).filter(
            df.trending_date.isin(week)).groupBy('video_id', 'category_id').agg(
            F.max(F.col('views')) - F.min(F.col('views')))
        this_week_videos = this_week_videos.filter(
            '(max(views) - min(views)) > 0')

        most_popular_cat = this_week_videos.groupBy('category_id').agg(
            F.sum('(max(views) - min(views))')).sort(desc('sum((max(views) - min(views)))')).limit(1).collect()

        if len(most_popular_cat) > 0:
            video_ids = [v[0] for v in this_week_videos.select('video_id').where(
                this_week_videos.category_id == most_popular_cat[0][0]).collect()]
            views = [int(v[0]) for v in df.filter((df.category_id == most_popular_cat[0][0]) & (df.video_id.isin(
                video_ids))).groupBy('views').agg(F.max(F.col('views'))).collect()]

            data.append([week[0], week[-1], most_popular_cat[0][0],
                         categories[most_popular_cat[0][0]], len(video_ids), sum(views), video_ids])

    res_df = spark.createDataFrame(data=data, schema=schema)
    res_df.printSchema()
    # res_df.show(truncate=False)

    res_df.write.mode("Overwrite").json("./second_q_schema")


def third_q(df):
    schema = StructType([
        StructField("start_date", StringType()),
        StructField("end_date", StringType()),
        StructField("tags", ArrayType(
            StructType([
                    StructField("tag", StringType()),
                    StructField("number_of_videos", LongType()),
                    StructField("video_ids", ArrayType(StringType()))
                    ])
        ))
    ])

    data = []
    dates = get_dates(df)

    month_len = 30
    for i in range(0, len(dates), month_len):
        if i+month_len > len(dates) - 1:
            month = dates[i:len(dates) - 1]
        else:
            month = dates[i:i+month_len]
        this_month_videos = df.filter(
            df.trending_date.isin(month)).groupBy('video_id').count()
        video_ids = [v[0]
                     for v in this_month_videos.select("video_id").collect()]
        valid_videos = df.select(df.video_id, df.tags).where(
            df.video_id.isin(video_ids) & df.trending_date.isin(month))

        tags_videos = {}
        tags_count = {}

        for v in valid_videos.collect():
            for tag in v[1]:
                if tag not in tags_videos:
                    tags_videos[tag] = []
                    tags_count[tag] = 0
                tags_videos[tag].append(v[0])
                tags_count[tag] += 1

        tags_count = {k: v for k, v in sorted(
            tags_count.items(), key=itemgetter(1), reverse=True)}
        popular_tags = []
        i = 0
        for t in tags_count:
            if i == 10:
                break
            popular_tags.append(
                [t, len(set(tags_videos[t])), list(set(tags_videos[t]))])
            i += 1

        data.append([month[0], month[-1], popular_tags])

    res_df = spark.createDataFrame(data=data, schema=schema)
    res_df.printSchema()
    # res_df.show(truncate=False)

    res_df.write.mode("Overwrite").json("./third_q_schema")


def third_q(df):
    schema = StructType([
        StructField("start_date", StringType()),
        StructField("end_date", StringType()),
        StructField("tags", ArrayType(
            StructType([
                    StructField("tag", StringType()),
                    StructField("number_of_videos", LongType()),
                    StructField("video_ids", ArrayType(StringType()))
                    ])
        ))
    ])

    data = []
    dates = get_dates(df)

    month_len = 30
    for i in range(0, len(dates), month_len):
        if i+month_len > len(dates) - 1:
            month = dates[i:len(dates) - 1]
        else:
            month = dates[i:i+month_len]
        this_month_videos = df.filter(
            df.trending_date.isin(month)).groupBy('video_id', 'tags').agg(F.count(F.col('tags')))

        tags_videos = {}
        for v in this_month_videos.collect():
            cur_tags = v[1].split('|')
            for t in cur_tags:
                if t not in tags_videos:
                    tags_videos[t] = []
                tags_videos[t].append(v[0])

        tags_count = []
        for tag in tags_videos:
            tags_count.append([tag, len(set(tags_videos[tag]))])

        top10_tags = sorted(tags_count, key=lambda x: x[1], reverse=True)[:10]

        tags = []
        for tag in top10_tags:
            videos = list(set(tags_videos[tag[0]]))
            num_of_videos = len(videos)
            tag_ls = [tag[0], num_of_videos, videos]
            tags.append(tag_ls)

        data.append([month[0], month[-1], tags])

    res_df = spark.createDataFrame(data=data, schema=schema)
    res_df.printSchema()
    # res_df.show(truncate=False)

    res_df.write.mode("Overwrite").json("./third_q_schema")


def fourth_q(df):
    schema = StructType([
        StructField("channel_name", StringType()),
        StructField("start_date", StringType()),
        StructField("end_date", StringType()),
        StructField("total_views", LongType()),
        StructField("videos_views", ArrayType(
            StructType([
                    StructField("video_id", StringType()),
                    StructField("views", LongType())
                    ])
        ))
    ])

    data = []

    dates = get_dates(df)

    channel_max_views = df.withColumn("views", col("views").cast("int")).groupBy(df.channel_title, df.video_id).agg(
        F.max(F.col("views"))).sort(desc('max(views)'))

    channel_max_views.show(truncate=False)

    i = 0
    for ch in channel_max_views.collect():
        if i == 20:
            break

        temp_df = df.select(df.video_id, df.views).where(
            df.channel_title == ch[0]).distinct()
        video_views = {}
        for c in temp_df.collect():
            if c[0] not in video_views or video_views[c[0]] < int(c[1]):
                video_views[c[0]] = int(c[1])
        total_views = sum(video_views.values())
        data.append([ch[0], dates[0], dates[-1],
                     total_views, list(video_views.items())])
        i += 1

    res_df = spark.createDataFrame(data=data, schema=schema)
    res_df.printSchema()
    # res_df.show(truncate=False)

    res_df.write.mode("Overwrite").json("./fourth_q_schema")


def fifth_q(df):
    schema = StructType([
        StructField("channel_name", StringType()),
        StructField("total_trending_days", LongType()),
        StructField("video_days", ArrayType(
            StructType([
                    StructField("video_id", StringType()),
                    StructField("video_title", StringType()),
                    StructField("trending_days", LongType())
                    ])
        ))
    ])

    data = []

    grouped_df = df.groupBy('channel_title').agg(
        F.count(F.col("trending_date"))).sort(desc("count(trending_date)")).limit(10)
    channel_titles = [[t[0], t[1]] for t in grouped_df.select(
        "channel_title", "count(trending_date)").collect()]

    for ch in channel_titles:
        ch_title, days = ch[0], ch[1]
        temp_df = df.filter(df.channel_title == ch_title).groupBy(
            "video_id", "title").agg(F.count(F.col("trending_date")))

        video_days = []
        for v in temp_df.collect():
            id, title, count = v[0], v[1], v[2]
            video_days.append([id, title, count])

        data.append([ch_title, days, video_days])

    res_df = spark.createDataFrame(data=data, schema=schema)
    res_df.printSchema()
    # res_df.show(truncate=False)

    res_df.write.mode("Overwrite").json("./fifth_q_schema")


def sixth_q(df):
    schema = StructType([
        StructField("category_id", IntegerType()),
        StructField("category_name", StringType()),
        StructField("videos", ArrayType(
            StructType([
                    StructField("video_id", StringType()),
                    StructField("video_title", StringType()),
                    StructField("ratio_likes_dislikes", DoubleType()),
                    StructField("views", LongType())
                    ])
        ))
    ])

    categories = get_categories()

    data = []

    for cat_id in categories:
        grouped_df = df.withColumn("views", col("views").cast("int")).filter((df.views > 100000) & (df.category_id == cat_id)).groupBy(
            'video_id', 'title', 'views').agg(F.max(F.col("likes") / F.col("dislikes"))).sort(desc("max((likes / dislikes))")).limit(10)

        videos = []
        for id, title, views, ratio in grouped_df.collect():
            videos.append([id, title, ratio, views])

        data.append([int(cat_id), categories[cat_id], videos])

    res_df = spark.createDataFrame(data=data, schema=schema)
    res_df.printSchema()
    # res_df.show(truncate=False)

    res_df.write.mode("Overwrite").json("./sixth_q_schema")


def count_rows(df):
    print(
        f"---------------------------------------------------------------------------------\nCount is: {df.count()}\n---------------------------------------------------------------------------------")


def print_q(i):
    print(
        f"---------------------------------------------------------------------------------\nQuestion number: {i}\n---------------------------------------------------------------------------------")


if __name__ == "__main__":
    file = "./CAvideos.csv"
    df = spark.read.csv(file, header=True).na.drop(
        subset=["description", "title", "category_id", "trending_date", "views"])
    df = df.filter(col("views").cast("int").isNotNull())

    print_q(1)
    first_q(df)
    print_q(2)
    second_q(df)
    print_q(3)
    third_q(df)
    print_q(4)
    fourth_q(df)
    print_q(5)
    fifth_q(df)
    print_q(6)
    sixth_q(df)
