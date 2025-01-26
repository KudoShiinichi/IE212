import re
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml import Pipeline

def clean_text(comment):
    """Làm sạch văn bản."""
    comment = comment.lower()
    comment = re.sub(r"[^\w\s]", "", comment)
    return comment

def create_pipeline():
    """Tạo pipeline xử lý dữ liệu."""
    tokenizer = Tokenizer(inputCol="comment", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    vectorizer = CountVectorizer(inputCol="filtered_words", outputCol="features")
    return Pipeline(stages=[tokenizer, remover, vectorizer])
