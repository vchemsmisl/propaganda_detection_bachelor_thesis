import pandas as pd
import re
from parser.constants import (
    ARTICLES_PATH,
    LINKS_PATH,
    DATASET_PATH,
    PREPROCESSED_DATASET_PATH
    )
from pymystem3 import mystem
import nltk
nltk.download('stopwords')


class DataCollector:

    def __init__(self, articles_path, links_path, dataset_path):

        self.Izvestiya_articles_armiia = articles_path / 'Izvestiya_articles_armiia'
        self.Izvestiya_articles_mir = articles_path / 'Izvestiya_articles_mir'
        self.Izvestiya_articles_politika = articles_path / 'Izvestiya_articles_politika'
        self.MK_articles = articles_path / 'MK_articles'
        self.RG_articles_gos = articles_path / 'RG_articles_gos'
        self.RG_articles_mir = articles_path / 'RG_articles_mir'

        self.articles_path = articles_path
        self.links_path = links_path
        self.dataset_path = dataset_path

    def create_links_table(self):
        Izvestiya_articles_armiia_links = [re.findall(r'Izvestiya_articles_armiia\\.*', str(link))[0]
                                           for link in self.Izvestiya_articles_armiia.glob('*')]
        Izvestiya_articles_mir_links = [re.findall(r'Izvestiya_articles_mir\\.*', str(link))[0]
                                           for link in self.Izvestiya_articles_mir.glob('*')]
        Izvestiya_articles_politika_links = [re.findall(r'Izvestiya_articles_politika\\.*', str(link))[0]
                                           for link in self.Izvestiya_articles_politika.glob('*')]
        MK_articles_links = [re.findall(r'MK_articles\\.*', str(link))[0]
                            for link in self.MK_articles.glob('*')]
        RG_articles_gos_links = [re.findall(r'RG_articles_gos\\.*', str(link))[0]
                                for link in self.RG_articles_gos.glob('*')]
        RG_articles_mir_links = [re.findall(r'RG_articles_mir\\.*', str(link))[0]
                                for link in self.RG_articles_mir.glob('*')]

        links_dataframe = pd.DataFrame(
            {
                'articles_links': Izvestiya_articles_armiia_links + \
                                  Izvestiya_articles_mir_links + \
                                  Izvestiya_articles_politika_links + \
                                  MK_articles_links + \
                                  RG_articles_gos_links + \
                                  RG_articles_mir_links
            }
        )
        links_dataframe.to_excel(self.links_path)


    @staticmethod
    def _get_info_from_article(link):
        path_to_article = ARTICLES_PATH / link

        with open(str(path_to_article), 'r', encoding='utf=8') as article_file:
            text = article_file.read()

        try:
            headline = text.split('\n')[0]
        except AttributeError:
            headline = ''

        source = link.split('_')[0]
        match source:
            case 'Izvestiya':
                source_name = 'Известия'
            case 'MK':
                source_name = 'Московский комсомолец'
            case 'RG':
                source_name = 'Российская газета'

        return pd.Series([link, headline, text, source_name])

    def create_texts_table(self):
        links_df = pd.read_excel(self.links_path).drop(columns='Unnamed: 0')
        texts_df = pd.DataFrame([], index=links_df.index,
                     columns=['link', 'headline', 'text', 'source'])

        texts_df[['link', 'headline', 'text', 'source']] = links_df['articles_links'].apply(self._get_info_from_article)
        return texts_df

    def preprocess_and_save_data(self, df):

        preprocessed_df = (
            df
            .drop_duplicates()
            .replace('', pd.NA)
            .dropna()
            .reset_index(drop=True)
        )

        preprocessed_df.to_excel(self.dataset_path)


class DataPreprocessor:

    def __init__(self, data_path):
        self._raw_data = pd.read_excel(str(data_path)).drop(columns='Unnamed: 0')
        self._preprocessed_data = None
        self._word_document_matrix = None

        self._analyzer = mystem.Mystem()
        self._stop_words_rus = nltk.corpus.stopwords.words('russian')

    def _preprocess_text(self, text):
        text_no_enter = text.lower().replace('\n', ' ')
        text_no_hyphen = text_no_enter.replace('-', ' ')
        text_no_punct = re.sub(r"[!\"#$%&\\'()*+,\-—.\/:;<=>«»?@[\]\\^_`{|}~]",
                               '', text_no_hyphen)

        text_lemmatized = self._analyzer.lemmatize(text_no_punct)

        text_without_stopwords = [word for word in text_lemmatized
                                  if word not in self._stop_words_rus
                                  and len(word) > 2]

        return ' '.join(text_without_stopwords)

    def preprocess_data(self):
        self._preprocessed_data = self._raw_data.copy()
        self._preprocessed_data['preprocessed_text'] = self._preprocessed_data['text'].apply(self._preprocess_text)

    def save_preprocessed_data(self, preproc_data_path):
        self._preprocessed_data.to_excel(str(preproc_data_path))


def collect_dataset():

    data_collector = DataCollector(ARTICLES_PATH, LINKS_PATH, DATASET_PATH)

    if not LINKS_PATH.exists():
        data_collector.create_links_table()

    df = data_collector.create_texts_table()
    data_collector.preprocess_and_save_data(df)


def preprocess_texts():

    data_preprocessor = DataPreprocessor(DATASET_PATH)

    data_preprocessor.preprocess_data()
    data_preprocessor.save_preprocessed_data(PREPROCESSED_DATASET_PATH)


if __name__ == '__main__':
    # collect_dataset()
    preprocess_texts()