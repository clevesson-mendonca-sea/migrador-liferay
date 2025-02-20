"""
Utility functions for handling hierarchy data structures.
"""

def parse_hierarchy(hierarchy_str: str) -> list:
    if not hierarchy_str:
        return ['Raiz']
    return [x.strip() for x in hierarchy_str.split('>')]

def filter_hierarchy(hierarchy_str: str) -> list:
    """
    Filtra a hierarquia ignorando os termos específicos.
    Retorna apenas os itens válidos.
    """
    if not hierarchy_str:
        return []
        
    ignored_terms = {'raiz', 'hierarquia'}
    return [
        item.strip() 
        for item in hierarchy_str.split('>')
        if item.strip().lower() not in ignored_terms
    ]
