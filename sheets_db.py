import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def format_timestamp(dt: datetime = None) -> str:
    """Format datetime to DD-MM-YYYY HH:MM:SS AM/PM"""
    if dt is None:
        dt = datetime.utcnow()
    return dt.strftime('%d-%m-%Y %I:%M:%S %p')

class SheetsDB:
    def __init__(self, credentials_path: str, sheet_id: str):
        self.credentials_path = credentials_path
        self.sheet_id = sheet_id
        self.client = None
        self.spreadsheet = None
        self._connect()
        self._initialize_sheets()
    
    def _connect(self):
        """Connect to Google Sheets"""
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_path, scope
        )
        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open_by_key(self.sheet_id)
        logger.info("Connected to Google Sheets successfully")
    
    def _initialize_sheets(self):
        """Initialize all required sheets with headers"""
        sheets_config = {
            'Settings': ['starting_balance', 'created_at'],
            'Categories': ['_id', 'name', 'type', 'is_preset', 'subcategories'],
            'Transactions': [
                '_id', 'timestamp', 'type', 'category', 'subcategory', 
                'amount', 'frequency', 'payment_mode', 'notes', 
                'month', 'year', 'debt_person', 'emi_id'
            ],
            'EMIs': [
                '_id', 'name', 'total_amount', 'paid_amount', 
                'remaining_amount', 'monthly_emi', 'start_date', 
                'end_date', 'status', 'created_at'
            ],
            'Debts': [
                '_id', 'person_name', 'type', 'total_amount', 
                'paid_amount', 'remaining_amount', 'created_at', 'updated_at'
            ]
        }
        
        existing_sheets = [ws.title for ws in self.spreadsheet.worksheets()]
        
        for sheet_name, headers in sheets_config.items():
            if sheet_name not in existing_sheets:
                worksheet = self.spreadsheet.add_worksheet(
                    title=sheet_name, 
                    rows=1000, 
                    cols=len(headers)
                )
                worksheet.append_row(headers)
                logger.info(f"Created sheet: {sheet_name}")
            else:
                worksheet = self.spreadsheet.worksheet(sheet_name)
                # Check if headers exist
                existing_headers = worksheet.row_values(1)
                if not existing_headers:
                    worksheet.append_row(headers)
                    logger.info(f"Added headers to existing sheet: {sheet_name}")
    
    def _get_worksheet(self, name: str):
        """Get worksheet by name"""
        return self.spreadsheet.worksheet(name)
    
    def _generate_id(self) -> str:
        """Generate a simple ID"""
        return datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
    
    def _row_to_dict(self, headers: List[str], row: List[Any]) -> Dict[str, Any]:
        """Convert a row to dictionary"""
        result = {}
        for i, header in enumerate(headers):
            value = row[i] if i < len(row) else ''
            
            # Convert strings to appropriate types
            if header in ['amount', 'total_amount', 'paid_amount', 'remaining_amount', 'monthly_emi', 'starting_balance']:
                try:
                    result[header] = float(value) if value else 0.0
                except:
                    result[header] = 0.0
            elif header == 'year':
                try:
                    result[header] = int(value) if value else 0
                except:
                    result[header] = 0
            elif header == 'is_preset':
                result[header] = value == 'True' or value == True
            elif header == 'subcategories':
                # Handle list - stored as comma-separated
                result[header] = value.split(',') if value else []
            else:
                result[header] = value
        
        return result
    
    # Settings operations
    def get_settings(self) -> Dict[str, Any]:
        """Get settings"""
        ws = self._get_worksheet('Settings')
        rows = ws.get_all_values()
        
        if len(rows) > 1:  # Has data besides header
            headers = rows[0]
            data_row = rows[1]
            return self._row_to_dict(headers, data_row)
        
        return {'starting_balance': 0}
    
    def save_settings(self, starting_balance: float) -> Dict[str, Any]:
        """Save or update settings"""
        ws = self._get_worksheet('Settings')
        rows = ws.get_all_values()
        
        timestamp = format_timestamp()
        
        if len(rows) > 1:  # Update existing
            ws.update('A2:B2', [[starting_balance, timestamp]])
        else:  # Insert new
            ws.append_row([starting_balance, timestamp])
        
        return {'starting_balance': starting_balance, 'created_at': timestamp}
    
    # Category operations
    def get_categories(self, category_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all categories or filter by type"""
        ws = self._get_worksheet('Categories')
        rows = ws.get_all_values()
        
        if len(rows) <= 1:
            return []
        
        headers = rows[0]
        categories = []
        
        for row in rows[1:]:
            if row and row[0]:  # Has ID
                cat = self._row_to_dict(headers, row)
                if category_type is None or cat.get('type') == category_type:
                    categories.append(cat)
        
        return categories
    
    def add_category(self, name: str, cat_type: str, is_preset: bool, subcategories: List[str]) -> Dict[str, Any]:
        """Add a new category"""
        ws = self._get_worksheet('Categories')
        
        cat_id = self._generate_id()
        subcats_str = ','.join(subcategories)
        
        ws.append_row([cat_id, name, cat_type, str(is_preset), subcats_str])
        
        return {
            '_id': cat_id,
            'name': name,
            'type': cat_type,
            'is_preset': is_preset,
            'subcategories': subcategories
        }
    
    # Transaction operations
    def add_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Add a new transaction"""
        ws = self._get_worksheet('Transactions')
        
        trans_id = self._generate_id()
        transaction['_id'] = trans_id
        
        # Prepare row data in correct order
        headers = ws.row_values(1)
        row_data = []
        for header in headers:
            value = transaction.get(header, '')
            if value is None:
                value = ''
            row_data.append(str(value))
        
        ws.append_row(row_data)
        
        return transaction
    
    def get_transactions(self, trans_type: Optional[str] = None, 
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get transactions with optional filters"""
        ws = self._get_worksheet('Transactions')
        rows = ws.get_all_values()
        
        if len(rows) <= 1:
            return []
        
        headers = rows[0]
        transactions = []
        
        for row in rows[1:]:
            if row and row[0]:  # Has ID
                trans = self._row_to_dict(headers, row)
                
                # Apply filters
                if trans_type and trans.get('type') != trans_type:
                    continue
                
                transactions.append(trans)
        
        return transactions
    
    # EMI operations
    def add_emi(self, emi: Dict[str, Any]) -> Dict[str, Any]:
        """Add a new EMI"""
        ws = self._get_worksheet('EMIs')
        
        emi_id = self._generate_id()
        emi['_id'] = emi_id
        
        headers = ws.row_values(1)
        row_data = []
        for header in headers:
            value = emi.get(header, '')
            if value is None:
                value = ''
            row_data.append(str(value))
        
        ws.append_row(row_data)
        
        return emi
    
    def get_emis(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get EMIs"""
        ws = self._get_worksheet('EMIs')
        rows = ws.get_all_values()
        
        if len(rows) <= 1:
            return []
        
        headers = rows[0]
        emis = []
        
        for row in rows[1:]:
            if row and row[0]:  # Has ID
                emi = self._row_to_dict(headers, row)
                if status is None or emi.get('status') == status:
                    emis.append(emi)
        
        return emis
    
    def update_emi(self, emi_id: str, updates: Dict[str, Any]) -> bool:
        """Update an EMI"""
        ws = self._get_worksheet('EMIs')
        rows = ws.get_all_values()
        
        if len(rows) <= 1:
            return False
        
        headers = rows[0]
        
        for idx, row in enumerate(rows[1:], start=2):
            if row and row[0] == emi_id:
                # Update the row
                for key, value in updates.items():
                    if key in headers:
                        col_idx = headers.index(key) + 1
                        ws.update_cell(idx, col_idx, str(value))
                return True
        
        return False
    
    # Debt operations
    def add_debt(self, debt: Dict[str, Any]) -> Dict[str, Any]:
        """Add a new debt"""
        ws = self._get_worksheet('Debts')
        
        debt_id = self._generate_id()
        debt['_id'] = debt_id
        
        headers = ws.row_values(1)
        row_data = []
        for header in headers:
            value = debt.get(header, '')
            if value is None:
                value = ''
            row_data.append(str(value))
        
        ws.append_row(row_data)
        
        return debt
    
    def get_debts(self, debt_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get debts"""
        ws = self._get_worksheet('Debts')
        rows = ws.get_all_values()
        
        if len(rows) <= 1:
            return []
        
        headers = rows[0]
        debts = []
        
        for row in rows[1:]:
            if row and row[0]:  # Has ID
                debt = self._row_to_dict(headers, row)
                if debt_type is None or debt.get('type') == debt_type:
                    debts.append(debt)
        
        return debts
    
    def update_debt(self, person_name: str, debt_type: str, updates: Dict[str, Any]) -> bool:
        """Update a debt"""
        ws = self._get_worksheet('Debts')
        rows = ws.get_all_values()
        
        if len(rows) <= 1:
            return False
        
        headers = rows[0]
        
        for idx, row in enumerate(rows[1:], start=2):
            if row and len(row) > 1:
                person_idx = headers.index('person_name')
                type_idx = headers.index('type')
                
                if row[person_idx] == person_name and row[type_idx] == debt_type:
                    # Update the row
                    for key, value in updates.items():
                        if key in headers:
                            col_idx = headers.index(key) + 1
                            ws.update_cell(idx, col_idx, str(value))
                    return True
        
        return False
