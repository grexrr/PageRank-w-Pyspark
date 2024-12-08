from pyspark import SparkContext

sc = SparkContext()
links = sc.textFile("links.txt")

# Turn each line into a tuple of (page, [linked pages])
links = links.map(lambda x: (x.split(' ')[0], x.split(' ')[1:]))
print(f"Links: {links.collect()}")

N = links.count()
print(f"Number of pages: {N}")

#Create and initialize the ranks
ranks = links.map(lambda node: (node[0], 1.0 / N))
print(f"Ranks: {ranks.collect()}")
