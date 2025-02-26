import re
import logging
from typing import Optional, Union
from unidecode import unidecode

logger = logging.getLogger(__name__)

class FolderNameValidator:
    # Palavras reservadas do Windows que não podem ser usadas
    RESERVED_WORDS = {
        'con', 'prn', 'aux', 'nul',
        'com1', 'com2', 'com3', 'com4', 'com5', 'com6', 'com7', 'com8', 'com9',
        'lpt1', 'lpt2', 'lpt3', 'lpt4', 'lpt5', 'lpt6', 'lpt7', 'lpt8', 'lpt9'
    }

    # Caracteres que não são permitidos em nomes de pasta
    INVALID_CHARS = ['\\', '/', ':', '*', '?', '"', '<', '>', '|', '#', '@', '!', '$', '%', '^', '&', '+', '=', 'º', 'ª', '°', '–', '—']
    
    # Padrões inválidos no final do nome
    INVALID_ENDINGS = ['..', '.', '-', ' ']
    
    # Padrões inválidos em qualquer lugar
    INVALID_PATTERNS = ['../', '/..', '//', '\\\\']
    
    # Limites específicos por tipo de pasta
    FOLDER_TYPE_LIMITS = {
        'journal': 95,    # Pasta de conteúdo web
        'documents': 255  # Pasta de documentos/arquivos
    }
    
    DEFAULT_MAX_LENGTH = 90 

    @staticmethod
    def get_max_length(folder_type: Optional[str] = None) -> int:
        """
        Retorna o limite máximo de caracteres baseado no tipo de pasta.
        """
        if folder_type and folder_type in FolderNameValidator.FOLDER_TYPE_LIMITS:
            return FolderNameValidator.FOLDER_TYPE_LIMITS[folder_type]
        return FolderNameValidator.DEFAULT_MAX_LENGTH

    @staticmethod
    def simple_truncate(name: str, max_length: int) -> str:
        """
        Realiza um truncamento simples mantendo o início do nome.
        """
        if len(name) <= max_length:
            return name
            
        # Trunca simplesmente mantendo o início e adicionando "..."
        truncated = name[:max_length-3] + "..."
        
        logger.info(f"Nome truncado: '{name}' -> '{truncated}'")
        return truncated

    @staticmethod
    def validate_and_clean_folder_name(name: str, max_length: Optional[Union[int, str]] = None, folder_type: Optional[str] = None) -> Optional[str]:
        """
        Valida e limpa o nome da pasta de acordo com as regras do Liferay.
        """
        if not name or not name.strip():
            logger.error("Nome da pasta não pode ficar em branco")
            return None
            
        if max_length is None:
            actual_max_length = FolderNameValidator.get_max_length(folder_type)
        else:
            try:
                actual_max_length = int(max_length)
            except (ValueError, TypeError):
                logger.warning(f"max_length inválido ({max_length}), usando valor padrão")
                actual_max_length = FolderNameValidator.get_max_length(folder_type)
                
        # Remove espaços extras e limpa extremidades
        cleaned_name = name.strip()
        
        # Verifica palavras reservadas
        if cleaned_name.lower() in FolderNameValidator.RESERVED_WORDS:
            logger.error(f"Nome de pasta '{cleaned_name}' é uma palavra reservada")
            return None
            
        # Primeiro remove acentos e caracteres especiais mantendo a legibilidade
        cleaned_name = unidecode(cleaned_name)
        
        # Remove "Nº" e variantes explicitamente (comuns em títulos de documentos)
        cleaned_name = re.sub(r'N[º°\.]\s*', 'N', cleaned_name)
        
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
        
        # Se estiver muito longo, trunca
        if len(cleaned_name) > actual_max_length:
            cleaned_name = FolderNameValidator.simple_truncate(cleaned_name, actual_max_length)
            
        # Verifica tamanho final
        if len(cleaned_name) < 1:
            logger.error(f"Nome de pasta inválido após limpeza: {cleaned_name}")
            return None
            
        return cleaned_name

def normalize_folder_name(title: str, max_length: Optional[Union[int, str]] = None, folder_type: Optional[str] = None) -> str:
    """
    Normaliza o nome da pasta aplicando regras de formatação e validação do Liferay
    
    Args:
        title (str): Título original da pasta
        max_length (int ou str, optional): Comprimento máximo personalizado ou None para usar o limite padrão
        folder_type (str, optional): Tipo de pasta ('journal' ou 'documents') para usar o limite específico
        
    Returns:
        str: Nome normalizado e válido para o Liferay
    """
    if not title:
        return ''
        
    if max_length is None:
        actual_max_length = FolderNameValidator.get_max_length(folder_type)
    else:
        try:
            actual_max_length = int(max_length)
        except (ValueError, TypeError):
            logger.warning(f"max_length inválido ({max_length}), usando valor padrão")
            actual_max_length = FolderNameValidator.get_max_length(folder_type)
        
    # Primeira limpeza básica - remover caracteres extremamente problemáticos
    title = title.replace('/', '-').replace('\\', '-').replace(':', '-')
    
    # Palavras especiais de formatação
    cases = {
        'lower': {'de', 'da', 'do', 'das', 'dos', 'e', 'em'},
        'upper': {'df', 'gdf', 'sei', 'cig', 'cat'}
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
    
    # Valida e limpa o nome formatado com limite de tamanho
    validated_name = FolderNameValidator.validate_and_clean_folder_name(formatted_name, actual_max_length, folder_type)
    
    # Se a validação falhar, tenta uma versão mais simples
    if not validated_name:
        # Remove todos os caracteres especiais, deixando apenas letras, números e espaços
        simplified_name = re.sub(r'[^\w\s]', '-', formatted_name)
        simplified_name = re.sub(r'-+', '-', simplified_name) 
        validated_name = FolderNameValidator.validate_and_clean_folder_name(simplified_name, actual_max_length, folder_type)
    
    # Se ainda falhar, usa um nome genérico
    if not validated_name:
        simple_name = "Pasta_" + title[:20].replace(" ", "_")
        validated_name = simple_name[:actual_max_length]
        logger.warning(f"Usando nome genérico para pasta: '{title}' -> '{validated_name}'")
    
    return validated_name