# In-Python module
from enum import Enum

class DBFormatEnum(Enum):
    """  
        Represents the type of obtained data from databases.
    """
    
    LIST = "list"
    DICT = "dict"