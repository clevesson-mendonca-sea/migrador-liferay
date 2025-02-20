import aiohttp
import logging
from configs.config import Config

logger = logging.getLogger(__name__)

class SessionManager:
    """Base class for managing HTTP sessions across the application."""
    
    def __init__(self, config):
        """
        Initialize the session manager.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.session = None
        
    async def initialize_session(self):
        """Create and initialize an HTTP session with authentication."""
        if self.session is None:
            auth = aiohttp.BasicAuth(
                login=self.config.liferay_user,
                password=self.config.liferay_pass
            )
            
            self.session = aiohttp.ClientSession(
                auth=auth,
                headers={"Content-Type": "application/json"},
                connector=aiohttp.TCPConnector(ssl=False)
            )
            logger.debug("HTTP session initialized")
        return self.session
        
    async def close(self):
        """Close the HTTP session if it exists."""
        if self.session:
            await self.session.close()
            self.session = None
            logger.debug("HTTP session closed")
            
    async def get(self, url, **kwargs):
        """
        Perform a GET request with the session.
        
        Args:
            url (str): The URL to request
            **kwargs: Additional parameters to pass to session.get()
            
        Returns:
            aiohttp.ClientResponse: The response object
        """
        if not self.session:
            await self.initialize_session()
        return await self.session.get(url, **kwargs)
        
    async def post(self, url, **kwargs):
        """
        Perform a POST request with the session.
        
        Args:
            url (str): The URL to request
            **kwargs: Additional parameters to pass to session.post()
            
        Returns:
            aiohttp.ClientResponse: The response object
        """
        if not self.session:
            await self.initialize_session()
        return await self.session.post(url, **kwargs)
        
    async def put(self, url, **kwargs):
        """
        Perform a PUT request with the session.
        
        Args:
            url (str): The URL to request
            **kwargs: Additional parameters to pass to session.put()
            
        Returns:
            aiohttp.ClientResponse: The response object
        """
        if not self.session:
            await self.initialize_session()
        return await self.session.put(url, **kwargs)
        
    async def delete(self, url, **kwargs):
        """
        Perform a DELETE request with the session.
        
        Args:
            url (str): The URL to request
            **kwargs: Additional parameters to pass to session.delete()
            
        Returns:
            aiohttp.ClientResponse: The response object
        """
        if not self.session:
            await self.initialize_session()
        return await self.session.delete(url, **kwargs)