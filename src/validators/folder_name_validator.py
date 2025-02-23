import re
import logging
from typing import Optional
from unidecode import unidecode

logger = logging.getLogger(__name__)

class FolderNameValidator:
    RESERVED_WORDS = {
        'con', 'prn', 'aux', 'nul',
        'com1', 'com2', 'com3', 'com4', 'com5', 'com6', 'com7', 'com8', 'com9',
        'lpt1', 'lpt2', 'lpt3', 'lpt4', 'lpt5', 'lpt6', 'lpt7', 'lpt8', 'lpt9'
    }

    # Caracteres que não são permitidos em nomes de pasta
    INVALID_CHARS = ['\\', '/', ':', '*', '?', '"', '<', '>', '|', '!', '$', '%', '^', '&', '+']
    
    # Padrões inválidos no final do nome
    INVALID_ENDINGS = ['..', '.', '-', ' ']
    
    # Padrões inválidos em qualquer lugar
    INVALID_PATTERNS = ['../', '/..', '//']

    @staticmethod
    def validate_and_clean_folder_name(name: str) -> Optional[str]:
        """
        Valida e limpa o nome da pasta de acordo com as regras do Liferay.
        
        Args:
            name (str): Nome original da pasta
            
        Returns:
            Optional[str]: Nome limpo e válido ou None se inválido
        """
        if not name or not name.strip():
            logger.error("Nome da pasta não pode ficar em branco")
            return None
            
        # Remove espaços extras e limpa extremidades
        cleaned_name = name.strip()
        
        # Verifica palavras reservadas
        if cleaned_name.lower() in FolderNameValidator.RESERVED_WORDS:
            logger.error(f"Nome de pasta '{cleaned_name}' é uma palavra reservada")
            return None
            
        # Remove acentos e caracteres especiais
        cleaned_name = unidecode(cleaned_name)
        
        # Substitui caracteres inválidos por hífen
        for char in FolderNameValidator.INVALID_CHARS:
            cleaned_name = cleaned_name.replace(char, '-')
            
        # Remove caracteres de controle e outros caracteres problemáticos
        cleaned_name = ''.join(char for char in cleaned_name if ord(char) >= 32 and ord(char) != 127)
        
        # Substitui múltiplos espaços por um único espaço
        cleaned_name = re.sub(r'\s+', ' ', cleaned_name)
        
        # Remove padrões inválidos
        for pattern in FolderNameValidator.INVALID_PATTERNS:
            cleaned_name = cleaned_name.replace(pattern, '-')
        
        # Remove múltiplos hífens
        cleaned_name = re.sub(r'-+', '-', cleaned_name)
        
        # Remove terminações inválidas
        while any(cleaned_name.endswith(end) for end in FolderNameValidator.INVALID_ENDINGS):
            cleaned_name = cleaned_name.rstrip('.- ')
            
        # Remove sequências de pontos
        cleaned_name = re.sub(r'\.+', '.', cleaned_name)
        
        # Verifica tamanho final
        if len(cleaned_name) < 1 or len(cleaned_name) > 255:
            logger.error(f"Nome de pasta inválido após limpeza: {cleaned_name}")
            return None
            
        return cleaned_name

def normalize_folder_name(title: str) -> str:
    """
    Normaliza o nome da pasta aplicando regras de formatação e validação do Liferay
    
    Args:
        title (str): Título original da pasta
        
    Returns:
        str: Nome normalizado e válido para o Liferay
    """
    if not title:
        return ''
        
    # Palavras especiais de formatação
    cases = {
        'lower': {'de', 'da', 'do', 'das', 'dos', 'e', 'em'},
        'upper': {'df', 'gdf', 'sei', 'cig'}
    }
    
    # Primeira limpeza básica
    words = title.strip().split()
    if not words:
        return ''
        
    # Formata cada palavra
    def format_word(word: str, index: int) -> str:
        word = word.lower()
        if word in cases['upper']: return word.upper()
        if word in cases['lower'] and index > 0: return word
        return word.capitalize()
    
    # Junta as palavras formatadas
    formatted_name = ' '.join(format_word(w, i) for i, w in enumerate(words))
    
    # Valida e limpa o nome formatado
    validated_name = FolderNameValidator.validate_and_clean_folder_name(formatted_name)
    
    # Se a validação falhar, tenta uma versão mais simples
    if not validated_name:
        # Remove todos os caracteres não alfanuméricos exceto espaços
        simplified_name = re.sub(r'[^\w\s]', '', formatted_name)
        validated_name = FolderNameValidator.validate_and_clean_folder_name(simplified_name)
        
    return validated_name if validated_name else 'pasta_sem_nome'