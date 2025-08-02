from pydantic import BaseModel
from datetime import datetime
import pandas as pd
from typing import Dict, List, Optional, Any

class ETLSession:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.file_path: Optional[str] = None
        self.file_type: Optional[str] = None
        self.sheets: List[str] = []
        self.selected_sheet: Optional[str] = None
        self.dataframe: Optional[pd.DataFrame] = None
        self.columns: List[str] = []
        self.preview_data: List[Dict] = []
        self.column_mapping: Dict[str, str] = {}
        self.transformations: Dict[str, Any] = {}
        self.created_at = datetime.now()

class ETLConfig(BaseModel):
    name: str
    description: str
    column_mapping: Dict[str, str]
    transformations: Dict[str, Dict]
    target_table: str
    mode: str = 'insert'
    encoding: str = 'latin1'
