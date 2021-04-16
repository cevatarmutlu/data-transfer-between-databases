# In-Python module
from enum import Enum

class DBEnum(Enum):
    """  
        Database names that can be used in the project.
    """
    
    PostgreSQL      = "PostgreSQL"
    ElasticSearch   = "ElasticSearch"
