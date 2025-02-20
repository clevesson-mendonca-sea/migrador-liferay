import os

class Config:
    """Configuration class for Liferay migration project."""
    
    def __init__(self):
        self.liferay_url = os.getenv('LIFERAY_URL', '')
        self.liferay_user = os.getenv('LIFERAY_USERNAME', '')
        self.liferay_pass = os.getenv('LIFERAY_PASSWORD', '')
        self.site_id = os.getenv('LIFERAY_SITE_ID', '')
        self.sheet_id = os.getenv('SPREADSHEET_ID', '')
        self.folder_type = os.getenv('FOLDER_TYPE', 'journal')
        self.content_structure_id = os.getenv('LIFERAY_CONTENT_STRUCTURE_ID', '')
        self.colapse_structure_id = os.getenv('LIFERAY_COLAPSE_STRUCTURE_ID', '')
        self.display_template_key = os.getenv('LIFERAY_DISPLAY_TEMPLETE_KEY', '')