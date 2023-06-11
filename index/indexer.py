from nltk.stem.snowball import SnowballStemmer
from bs4 import BeautifulSoup
import string
from nltk.tokenize import word_tokenize
import os
from index.structure import *
from tqdm import tqdm


class Cleaner:
    def __init__(self, stop_words_file: str, language: str,
                 perform_stop_words_removal: bool, perform_accents_removal: bool,
                 perform_stemming: bool):
        self.set_stop_words = self.read_stop_words(stop_words_file)

        self.stemmer = SnowballStemmer(language)
        in_table = "áéíóúâêôçãẽõü"
        out_table = "aeiouaeocaeou"
        # altere a linha abaixo para remoção de acentos (Atividade 11)
        self.accents_translation_table = []
        for i in range(len(in_table)):
            tupla = (in_table[i], out_table[i])
            self.accents_translation_table.append(tupla)
        self.set_punctuation = set(string.punctuation)

        # flags
        self.perform_stop_words_removal = perform_stop_words_removal
        self.perform_accents_removal = perform_accents_removal
        self.perform_stemming = perform_stemming

    def html_to_plain_text(self, html_doc: str) -> str:
        content = BeautifulSoup(html_doc, 'html.parser')
        return content.get_text()

    @staticmethod
    def read_stop_words(str_file) -> set:
        set_stop_words = set()
        with open(str_file, encoding='utf-8') as stop_words_file:
            for line in stop_words_file:
                arr_words = line.split(",")
                [set_stop_words.add(word) for word in arr_words]
        return set_stop_words

    def is_stop_word(self, term: str):
        if term in self.set_stop_words:
            return True
        return False

    def word_stem(self, term: str):
        return "" + self.stemmer.stem(term)

    def remove_accents(self, term: str) -> str:
        for i in range(len(term)):
            for tupla in self.accents_translation_table:
                if term[i] == tupla[0]:
                    list_term = list(term)
                    list_term[i] = tupla[1]
                    term = ''.join(list_term)
        return term

    def preprocess_word(self, term: str) -> str or None:
        def is_punctuation(text):
            return all(char in string.punctuation for char in text)
        if is_punctuation(term):
            return None
        if self.perform_stop_words_removal:
            if self.is_stop_word(term):
                return None
        if self.perform_stemming:
            return self.word_stem(term)
        return term

    def preprocess_text(self, text: str) -> str or None:
        text = self.remove_accents(text)
        return text.lower()
class HTMLIndexer:
    cleaner = Cleaner(stop_words_file="stopwords.txt",
                      language="portuguese",
                      perform_stop_words_removal=True,
                      perform_accents_removal=True,
                      perform_stemming=False)

    def __init__(self, index):
        self.index = index

    def text_word_count(self, plain_text: str):
        dic_word_count = {}
        plain_text = self.cleaner.preprocess_text(plain_text)
        tokenized = word_tokenize(plain_text)
        for word in tokenized:
            valid_token = self.cleaner.preprocess_word(word)
            if valid_token is not None:
                if valid_token in dic_word_count:
                    dic_word_count[valid_token] = dic_word_count[valid_token] + 1
                else:
                    dic_word_count[valid_token] = 1
        #dic_word_count = dict(sorted(dic_word_count.items()))
        return dic_word_count

    def index_text(self, doc_id: int, text_html: str):
        plain_text = self.cleaner.html_to_plain_text(text_html)
        dic_word_count = self.text_word_count(plain_text)

        for x in dic_word_count:
            self.index.index(x, doc_id, dic_word_count[x])

        #self.index.dic_index = dict(sorted(self.index.dic_index.items()))
        self.index.finish_indexing()

    def index_text_dir(self, path: str):
        id=0
        for str_sub_dir in tqdm(os.listdir(path)):
            path_sub_dir = f"{path}/{str_sub_dir}"
            conteudo = os.listdir(path_sub_dir)
            for item in tqdm(conteudo):
                caminho_completo = os.path.join(path, str_sub_dir, item)
                caminho_completo = os.path.abspath(caminho_completo)
                with open(caminho_completo, "r", encoding="utf-8") as arquivo:
                    self.index_text(id, arquivo.read())
                id = id + 1
