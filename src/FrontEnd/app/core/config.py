import os
from typing import Literal
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    def __init__(self):
        pass

    @property
    def port(self) -> int:
        return int(os.getenv('PORT', '8000'))

    @property
    def host(self) -> str:
        return os.getenv('HOST', '0.0.0.0')

    @property
    def logging_mode(self) -> Literal['prod', 'info', 'debug']:
        mode = os.getenv('LOGGING_MODE', 'info').lower()
        if mode not in ['prod', 'info', 'debug']:
            return 'info'
        return mode

config = Config()
