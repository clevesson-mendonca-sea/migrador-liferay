import unicodedata
import re

class PageProcessor:
    @staticmethod
    def normalize_friendly_url(title: str) -> str:
        nfkd_form = unicodedata.normalize('NFKD', title)
        only_ascii = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
        friendly_url = re.sub(r'[^a-zA-Z0-9-]', '-', only_ascii)
        friendly_url = re.sub(r'-+', '-', friendly_url).strip('-').lower()
        return friendly_url

    @staticmethod
    def normalize_page_name(title: str) -> str:
        cases = {
            'lower': {'de', 'da', 'do', 'das', 'dos', 'e', 'é', 'em'},
            'upper': {'df', 'gdf', 'sei', 'cig'}
        }
        
        words = title.strip().split()
        if not words:
            return ''
            
        def format_word(word: str, index: int) -> str:
            word = word.lower()
            if word in cases['upper']: return word.upper()
            if word in cases['lower'] and index > 0: return word
            return word.capitalize()
        
        return ' '.join(format_word(w, i) for i, w in enumerate(words))
