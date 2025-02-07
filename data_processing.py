import numpy as np
import pandas as pd
from pyvi.ViTokenizer import ViTokenizer
import regex as re
import os
import matplotlib.pyplot as plt
import seaborn as sns
from emot.emo_unicode import UNICODE_EMOJI, EMOTICONS_EMO
from collections import Counter
import random


STOPWORDS = './data/vietnamese_stop_word/vietnamese-stopwords.txt'
abb_dict_normal_path = './data/dictionary/abb_dict_normal.xlsx'
abb_dict_special_path = './data/dictionary/abb_dict_special.xlsx'
emoji2word_path = './data/dictionary/emoji2word.xlsx'
character2emoji_path = './data/dictionary/character2emoji.xlsx'


with open(STOPWORDS, "r", encoding="utf-8") as ins:
    stopwords = []
    for line in ins:
        dd = line.strip('\n')
        stopwords.append(dd)
    stopwords = set(stopwords)

def filter_stop_words(train_sentences, stop_words):
    new_sent = [word for word in train_sentences.split() if word not in stop_words]
    train_sentences = ' '.join(new_sent)
    return train_sentences

def check_repeated_character(text):
    text = re.sub('  +', ' ', text).strip()
    count = {}
    for i in range(len(text) - 1):
        if text[i] == text[i + 1]:
            return True
    return False

def check_space(text):  # check space in string
    for i in range(len(text)):
        if text[i] == ' ':
            return True
    return False


def check_special_character_numberic(text):
    return any(not c.isalpha() for c in text)

def remove_emoji(text):
    for emot in UNICODE_EMOJI:
        text = str(text).replace(emot, ' ')
    text = re.sub('  +', ' ', text).strip()
    return text

# Remove url
def url(text):
    text = re.sub(r'https?://\S+|www\.\S+', ' ', str(text))
    text = re.sub('  +', ' ', text).strip()
    return text

# remove special character
def special_character(text):
    text = re.sub(r'\d+', lambda m: " ", text)
    # text = re.sub(r'\b(\w+)\s+\1\b',' ', text) #remove duplicate number word
    text = re.sub("[~!@#$%^&*()_+{}“”|:\"<>?`´\-=[\]\;\\\/.,]", " ", text)
    text = re.sub('  +', ' ', text).strip()
    return text

# normalize repeated characters
def repeated_character(text):
    text = re.sub(r'(\w)\1+', r'\1', text)
    text = re.sub('  +', ' ', text).strip()
    return text

def mail(text):
    text = re.sub(r'[^@]+@[^@]+\.[^@]+', ' ', text)
    text = re.sub('  +', ' ', text).strip()
    return text

# remove mention tag and hashtag
def tag(text):
    text = re.sub(r"(?:\@|\#|\://)\S+", " ", text)
    text = re.sub('  +', ' ', text).strip()
    return text

# """Remove all mixed words and numbers"""
def mixed_word_number(text):
    text = ' '.join(s for s in text.split() if not any(c.isdigit() for c in s))
    text = re.sub('  +', ' ', text).strip()
    return text

c2e_path = os.path.join(os.getcwd(), character2emoji_path)
character2emoji = pd.read_excel(c2e_path)  # character to emoji
def convert_character2emoji(text):
    text = str(text)
    for i in range(character2emoji.shape[0]):
        text = text.replace(character2emoji.at[i, 'character'], " " + character2emoji.at[i, 'emoji'] + " ")
    text = re.sub('  +', ' ', text).strip()
    return text

e2w_path = os.path.join(os.getcwd(), emoji2word_path)
emoji2word = pd.read_excel(e2w_path)  # emoji to word

def convert_emoji2word(text):
    for i in range(emoji2word.shape[0]):
        text = text.replace(emoji2word.at[i, 'emoji'], " " + emoji2word.at[i, 'word_vn'] + " ")
    text = re.sub('  +', ' ', text).strip()
    return text

adn_path = os.path.join(os.getcwd(), abb_dict_normal_path)
abb_dict_normal = pd.read_excel(adn_path)


def abbreviation_normal(text):  # len word equal 1
    text = str(text)
    temp = ''
    for word in text.split():
        for i in range(abb_dict_normal.shape[0]):
            if str(abb_dict_normal.at[i, 'abbreviation']) == str(word):
                word = str(abb_dict_normal.at[i, 'meaning'])
        temp = temp + ' ' + word
    text = temp
    text = re.sub('  +', ' ', text).strip()
    return text

ads_path = os.path.join(os.getcwd(), abb_dict_special_path)
abb_dict_special = pd.read_excel(ads_path)


def abbreviation_special(text):  # including special character and number
    text = ' ' + str(text) + ' '
    for i in range(abb_dict_special.shape[0]):
        text = text.replace(' ' + abb_dict_special.at[i, 'abbreviation'] + ' ',
                            ' ' + abb_dict_special.at[i, 'meaning'] + ' ')
    text = re.sub('  +', ' ', text).strip()
    return text

def special_character_1(text):  # remove dot and comma
    text = re.sub("[.,?!]", " ", text)
    text = re.sub('  +', ' ', text).strip()
    return text

def abbreviation_kk(text):
    text = str(text)
    for t in text.split():
        if 'kk' in t:
            text = text.replace(t, ' ha ha ')
        else:
            if 'kaka' in t:
                text = text.replace(t, ' ha ha ')
            else:
                if 'kiki' in t:
                    text = text.replace(t, ' ha ha ')
                else:
                    if 'haha' in t:
                        text = text.replace(t, ' ha ha ')
                    else:
                        if 'hihi' in t:
                            text = text.replace(t, ' ha ha ')
    text = re.sub('  +', ' ', text).strip()
    return text

def remove_quality_product(text):
    # Loại bỏ từ "Chất lượng sản phẩm"
    return text.replace("Chất lượng sản phẩm:", "").strip()

def remove_special_function(text):
    # Loại bỏ từ "Tính năng nổi bật"
    return text.replace("Tính năng nổi bật:", "").strip()


# tokenize by lib Pyvi
def tokenize(text):
    text = str(text)
    text = ViTokenizer.tokenize(text)
    return text


def preprocessing(text, lowercased = False):
    text = remove_quality_product(text)
    text = remove_special_function(text)
    text = filter_stop_words(text, stopwords)
    text = text.lower() 
    text = convert_character2emoji(text)
    text = url(text)
    text = mail(text)
    text = tag(text)
    text = mixed_word_number(text)
    text = special_character_1(text)  # ##remove , . ? !
    text = abbreviation_kk(text)
    text = abbreviation_special(text)
    text = convert_character2emoji(text)
    text = remove_emoji(text)
    text = repeated_character(text)
    text = special_character(text)
    text = abbreviation_normal(text)
    # text = abbreviation_predict(text)
    text = tokenize(text)
    return text