from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import length
from pyspark.sql.functions import regexp_replace, trim, col, lower
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import desc
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import re

spark = SparkSession.builder.appName("wordcountdf").master("local[*]").getOrCreate()
sc = spark.sparkContext
# create a silly test dataframe from Python collections (lists)
wordsDF = spark.createDataFrame([('look',), ('spark',), ('tutorial',), ('spark',), ('look',), ('python',)], ['word'])
wordsDF.show()
print(type(wordsDF))
wordsDF.printSchema()

# Create a new DataFrame from an existing one
# Spark Actions like show(), collect() or count() then cause Spark to execute the recipe to transform the source. It is the mechanism for getting results out of Spark.
# Length of each word
# You can create a new DataFrame from our base DF wordsDF by calling the select DataFrame function and pass in the appropriate recipe: we can use the SQL length function to find the number of characters in each word.
# The length function is found in the pyspark.sql.functions module.
wordsLengthsDF = wordsDF.select(length('word').alias('lengths'))  # transformation
wordsLengthsDF.show()  # action
#
# Now, let's count the number of times a particular word appears in the 'word' column. There are multiple ways to perform the counting, but some are much less efficient than others.
# A naive approach would be to call collect on all of the elements and count them in the driver program. While this approach could work for small datasets, we want an approach that will work for any size dataset including terabyte- or petabyte-sized datasets. In addition, performing all of the work in the driver program is slower than performing it in parallel in the workers. For these reasons, we will use data parallel operations.

#
# Using groupBy and count
# Using DataFrames, we can preform aggregations by grouping the data using the groupBy function on the DataFrame. Using groupBy returns a GroupedData object and we can use the functions available for GroupedData to aggregate the groups. For example, we can call avg or count on a GroupedData object to obtain the average of the values in the groups or the number of occurrences in the groups, respectively.
# To find the counts of words, we group by the words and then use the count function to find the number of times that words occur.


wordCountsDF = (wordsDF
                .groupBy('word').count())
wordCountsDF.show()

# Unique words
# Calculate the number of unique words in wordsDF.
uniqueWordsCount = wordCountsDF.count()
print(uniqueWordsCount)

# Means of groups using DataFrames
# Find the mean number of occurrences of words in wordCountsDF.

averageCount = (wordCountsDF
                .groupBy().mean('count')).collect()[0][0]
print(averageCount)


# In this section we will finish developing our word count application. We'll have to build the wordCount function, deal with real world problems like capitalization and punctuation, load in our data source, and compute the word count on the new data.
# The wordCount function
# First, we define a function for word counting.
#     This function takes in a DataFrame that is a list of words like wordsDF and returns a DataFrame that has all of the words and their associated counts.

def wordCount(wordListDF):
    """Creates a DataFrame with word counts.

    Args:
        wordListDF (DataFrame of str): A DataFrame consisting of one string column called 'word'.

    Returns:
        DataFrame of (str, int): A DataFrame containing 'word' and 'count' columns.
    """
    return (wordListDF
            .groupBy('word').count())


# apply the new function to the words DataFrame, it should get the same result
wordCount(wordsDF).show()


# Capitalization and punctuation
# Real world files are more complicated than the data we have been using until now. Some of the issues we have to address are:
# Words should be counted independent of their capitialization (e.g., Spark and spark should be counted as the same word).
# All punctuation should be removed.
# Any leading or trailing spaces on a line should be removed.
# We now define the function removePunctuation that converts all text to lower case, removes any punctuation, and removes leading and trailing spaces. We use the Python regexp_replace module to remove any text that is not a letter, number, or space and the trim and lower functions found in pyspark.sql.functions.
def removePunctuation(column):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        column (Column): A Column containing a sentence.

    Returns:
        Column: A Column named 'sentence' with clean-up operations applied.
    """
    return trim(lower(regexp_replace(column, '([^\s\w_]|_)+', ''))).alias('sentence')


sentenceDF = spark.createDataFrame([('Hi, you',),
                                    (' Look! No under_score!',),
                                    (' *      Remove punctuation then spaces  * ',)], ['sentence'])
# dispaly first original sentence
sentenceDF.show(truncate=False)
# then sentence with punctuation removed
(sentenceDF
 .select(removePunctuation(col('sentence')))
 .show(truncate=False))

#
# Load a text file
# For the next part, we will use the Complete Works of William Shakespeare from Project Gutenberg. To convert a text file into a DataFrame, we use the sqlContext.read.text() method. We also apply the recently defined removePunctuation() function using a select() transformation to strip out the punctuation and change all text to lower case. Since the file is large we use show(15), so that we only print 15 lines.
fileName = "PrideandPrejudice.txt"

bookDF = spark.read.text(fileName).select(removePunctuation(col('value')))
bookDF.show(15, truncate=False)

# Words from lines
# Before we can use the wordcount() function, we have to address two issues with the format of the DataFrame:
#     The first issue is that that we need to split each line by its spaces.
# The second issue is we need to filter out empty lines or words.
# We apply a transformation that will split each 'sentence' in the DataFrame by its spaces, and then transform from a DataFrame that contains lists of words into a DataFrame with each word in its own row. To accomplish these two tasks you can use the split and explode functions found in pyspark.sql.functions.
# Once we have a DataFrame with one word per row we can apply the DataFrame operation where to remove the rows that contain ''.

bookWordsSplitDF = (bookDF
                    .select(split(bookDF.sentence, ' ').alias('split')))
bookWordsSingleDF = (bookWordsSplitDF.select(explode(bookWordsSplitDF.split).alias('word')))
bookWordsDF = bookWordsSingleDF.where(bookWordsSingleDF.word != '')
bookWordsDF.show()
bookWordsDFCount = bookWordsDF.count()
print(bookWordsDFCount)

#
# Count the words
# We now have a DataFrame that is only words. Next, let's apply the wordCount() function to produce a list of word counts. We can view the first 20 words by using the show() action; however, we'd like to see the words in descending order of count, so we'll need to apply the orderBy DataFrame method to first sort the DataFrame that is returned from wordCount().
# You'll notice that many of the words are common English words. These are called stopwords. We will see how to eliminate them from the results.
WordsAndCountsDF = wordCount(bookWordsDF)
topWordsAndCountsDF = WordsAndCountsDF.orderBy("count", ascending=0)

topWordsAndCountsDF.show()
#
# Removing stopwords
# Stopwords are common (English) words that do not contribute much to the content or meaning of a document (e.g., "the", "a", "is", "to", etc.). Stopwords add noise to bag-of-words comparisons, so they are usually excluded. Using the included file "stopwords.txt", implement tokenize, an improved tokenizer that does not emit stopwords.
# In Python, we can test membership in a set as follows:
# my_set = set(['a', 'b', 'c'])
# 'a' in my_set     # returns True
# 'd' in my_set     # returns False
# 'a' not in my_set # returns False
# Within tokenize(), first tokenize the string using simpleTokenize(). Then, remove stopwords. To remove stop words, consider using a loop, a Python list comprehension, or the built-in Python filter() function.

import os

stopwords = set(sc.textFile("PrideandPrejudice.txt").collect())
# print('These are the stopwords: %s' % stopwords)
# These are the stopwords: set([u'all', u'just', u'being', u'over', u'both', u'through', u'yourselves', u'its', u'before', u'with', u'had', u'should', u'to', u'only', u'under', u'ours', u'has', u'do', u'them', u'his', u'very', u'they', u'not', u'during', u'now', u'him', u'nor', u'did', u'these', u't', u'each', u'where', u'because', u'doing', u'theirs', u'some', u'are', u'our', u'ourselves', u'out', u'what', u'for', u'below', u'does', u'above', u'between', u'she', u'be', u'we', u'after', u'here', u'hers', u'by', u'on', u'about', u'of', u'against', u's', u'or', u'own', u'into', u'yourself', u'down', u'your', u'from', u'her', u'whom', u'there', u'been', u'few', u'too', u'themselves', u'was', u'until', u'more', u'himself', u'that', u'but', u'off', u'herself', u'than', u'those', u'he', u'me', u'myself', u'this', u'up', u'will', u'while', u'can', u'were', u'my', u'and', u'then', u'is', u'in', u'am', u'it', u'an', u'as', u'itself', u'at', u'have', u'further', u'their', u'if', u'again', u'no', u'when', u'same', u'any', u'how', u'other', u'which', u'you', u'who', u'most', u'such', u'why', u'a', u'don', u'i', u'having', u'so', u'the', u'yours', u'once'])
type(stopwords)
quickbrownfox = 'A quick brown fox jumps over the lazy dog.'
split_regex = r'\W+'

def simpleTokenise(string):
    """ A simple implementation of input string tokenization
    Args:
        string (str): input string
    Returns:
        list: a list of tokens
    """
    return filter(None, re.split(split_regex, string.lower()))

# print(simpleTokenise(quickbrownfox))  # Should give ['a', 'quick', 'brown', ... ]


def removeStopWords(listOfTokens):
    return [token for token in listOfTokens if token not in stopwords]
#
#
def tokeniseAndRemoveStopwords(string):
    """ An implementation of input string tokenization that excludes stopwords
    Args:
        string (str): input string
    Returns:
        list: a list of tokens without stopwords
    """
    tmpLista = simpleTokenise(string)
    return removeStopWords(tmpLista)
#
#
# print(tokeniseAndRemoveStopwords(quickbrownfox))
# # Should give ['quick', 'brown', ... ]
#
removeStopWords_udf = udf(removeStopWords, ArrayType(StringType()))
bookWordsNoStopDF = (bookWordsSplitDF.select(removeStopWords_udf("split").alias('wordsNoStop')))

bookWordsSingleDF = (bookWordsNoStopDF
                     .select(explode(bookWordsNoStopDF.wordsNoStop).alias('word')))
bookWordsDF = bookWordsSingleDF.where(bookWordsSingleDF.word != '')
bookWordsDF.show()
#
# print(bookWordsDF.count())
#
# WordsAndCountsDF = wordCount(bookWordsDF)
# topWordsAndCountsDF = WordsAndCountsDF.orderBy("count", ascending=0)
#
# topWordsAndCountsDF.show()
