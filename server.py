from fastapi import FastAPI, APIRouter, HTTPException
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime, timedelta
from sheets_db import SheetsDB, format_timestamp

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# Initialize Google Sheets DB
GOOGLE_SHEET_ID = os.environ['GOOGLE_SHEET_ID']
GOOGLE_CREDENTIALS_PATH = os.environ['GOOGLE_CREDENTIALS_PATH']

db = SheetsDB(GOOGLE_CREDENTIALS_PATH, GOOGLE_SHEET_ID)

# Create the main app without a prefix
app = FastAPI()

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Pydantic Models
class SettingsUpdate(BaseModel):
    starting_balance: float

class CategoryCreate(BaseModel):
    name: str
    type: str
    subcategories: List[str] = []

class TransactionCreate(BaseModel):
    type: str
    category: str
    subcategory: Optional[str] = None
    amount: float
    frequency: str
    payment_mode: str
    notes: Optional[str] = None
    month: str
    year: int
    debt_person: Optional[str] = None
    emi_id: Optional[str] = None

class EMICreate(BaseModel):
    name: str
    total_amount: float
    monthly_emi: float
    start_date: str
    end_date: str

# Initialize preset categories
def init_preset_categories():
    preset_categories = [
        # Income categories
        {"name": "Salary", "type": "income", "is_preset": True, "subcategories": ["Monthly Salary", "Bonus", "Incentive"]},
        {"name": "Business", "type": "income", "is_preset": True, "subcategories": ["Sales", "Services", "Profit"]},
        {"name": "Freelance", "type": "income", "is_preset": True, "subcategories": ["Project", "Consulting", "Gig"]},
        {"name": "Investment", "type": "income", "is_preset": True, "subcategories": ["Dividend", "Interest", "Capital Gain"]},
        {"name": "Other Income", "type": "income", "is_preset": True, "subcategories": ["Gift", "Refund", "Miscellaneous"]},
        
        # Expense categories
        {"name": "Food", "type": "expense", "is_preset": True, "subcategories": ["Groceries", "Dining Out", "Snacks", "Home Cooking"]},
        {"name": "Transport", "type": "expense", "is_preset": True, "subcategories": ["Fuel", "Public Transport", "Auto/Taxi", "Maintenance"]},
        {"name": "Bills", "type": "expense", "is_preset": True, "subcategories": ["Electricity", "Water", "Internet", "Phone", "Rent"]},
        {"name": "Shopping", "type": "expense", "is_preset": True, "subcategories": ["Clothing", "Electronics", "Home Items", "Personal Care"]},
        {"name": "Entertainment", "type": "expense", "is_preset": True, "subcategories": ["Movies", "Games", "Subscriptions", "Hobbies"]},
        {"name": "Health", "type": "expense", "is_preset": True, "subcategories": ["Medicine", "Doctor", "Gym", "Insurance"]},
        {"name": "Education", "type": "expense", "is_preset": True, "subcategories": ["Fees", "Books", "Courses", "Supplies"]},
        {"name": "Other Expense", "type": "expense", "is_preset": True, "subcategories": ["Gifts", "Donation", "Miscellaneous"]},
    ]
    
    # Check if categories already exist
    existing = db.get_categories()
    if len(existing) == 0:
        for cat in preset_categories:
            db.add_category(
                name=cat["name"],
                cat_type=cat["type"],
                is_preset=cat["is_preset"],
                subcategories=cat["subcategories"]
            )
        logger.info("Preset categories initialized in Google Sheets")

# Initialize on startup
@app.on_event("startup")
async def startup_event():
    init_preset_categories()
    logger.info("Application started with Google Sheets backend")

# Settings endpoints
@api_router.post("/settings")
async def create_or_update_settings(settings: SettingsUpdate):
    result = db.save_settings(settings.starting_balance)
    return {"message": "Settings saved", "starting_balance": result['starting_balance']}

@api_router.get("/settings")
async def get_settings():
    settings = db.get_settings()
    return settings

# Category endpoints
@api_router.get("/categories")
async def get_categories(type: Optional[str] = None):
    categories = db.get_categories(type)
    return categories

@api_router.post("/categories")
async def create_category(category: CategoryCreate):
    # Check if category already exists
    existing = db.get_categories(category.type)
    if any(cat['name'] == category.name for cat in existing):
        raise HTTPException(status_code=400, detail="Category already exists")
    
    result = db.add_category(
        name=category.name,
        cat_type=category.type,
        is_preset=False,
        subcategories=category.subcategories
    )
    return result

# Transaction endpoints
@api_router.post("/transactions")
async def create_transaction(transaction: TransactionCreate):
    trans_dict = transaction.dict()
    trans_dict['timestamp'] = format_timestamp()
    
    result = db.add_transaction(trans_dict)
    
    # Update debt if this is a debt transaction
    if transaction.type in ["Debt Given", "Debt Received"] and transaction.debt_person:
        debt_type = "given" if transaction.type == "Debt Given" else "received"
        existing_debts = db.get_debts(debt_type)
        existing_debt = next(
            (d for d in existing_debts if d['person_name'] == transaction.debt_person),
            None
        )
        
        if existing_debt:
            new_total = existing_debt["total_amount"] + transaction.amount
            new_remaining = existing_debt["remaining_amount"] + transaction.amount
            db.update_debt(
                person_name=transaction.debt_person,
                debt_type=debt_type,
                updates={
                    "total_amount": new_total,
                    "remaining_amount": new_remaining,
                    "updated_at": format_timestamp()
                }
            )
        else:
            db.add_debt({
                "person_name": transaction.debt_person,
                "type": debt_type,
                "total_amount": transaction.amount,
                "paid_amount": 0,
                "remaining_amount": transaction.amount,
                "created_at": format_timestamp(),
                "updated_at": format_timestamp()
            })
    
    # Update EMI if this is an EMI payment
    if transaction.type == "EMI" and transaction.emi_id:
        emis = db.get_emis()
        emi = next((e for e in emis if e['_id'] == transaction.emi_id), None)
        
        if emi:
            new_paid = emi["paid_amount"] + transaction.amount
            new_remaining = emi["total_amount"] - new_paid
            status = "completed" if new_remaining <= 0 else "active"
            
            db.update_emi(
                emi_id=transaction.emi_id,
                updates={
                    "paid_amount": new_paid,
                    "remaining_amount": new_remaining,
                    "status": status
                }
            )
    
    return result

@api_router.get("/transactions")
async def get_transactions(
    type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 1000
):
    transactions = db.get_transactions(type, start_date, end_date)
    # Sort by timestamp descending
    transactions.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
    return transactions[:limit]

# Dashboard endpoint
@api_router.get("/dashboard/summary")
async def get_dashboard_summary(period: str = "monthly"):
    # Calculate date range based on period
    now = datetime.utcnow()
    
    if period == "daily":
        start_date = datetime(now.year, now.month, now.day).isoformat()
    elif period == "weekly":
        start_date = (now - timedelta(days=7)).isoformat()
    else:  # monthly
        start_date = datetime(now.year, now.month, 1).isoformat()
    
    # Get all transactions
    all_transactions = db.get_transactions()
    
    # Filter transactions for the period
    period_transactions = [
        t for t in all_transactions 
        if t.get('timestamp', '') >= start_date
    ]
    
    # Calculate totals
    total_income = sum(t["amount"] for t in period_transactions if t["type"] == "Income")
    total_expense = sum(t["amount"] for t in period_transactions if t["type"] == "Expense")
    total_savings_all = sum(t["amount"] for t in all_transactions if t["type"] == "Savings")
    
    # Get all-time debt totals
    total_debt_given = sum(t["amount"] for t in all_transactions if t["type"] == "Debt Given")
    total_debt_received = sum(t["amount"] for t in all_transactions if t["type"] == "Debt Received")
    
    # Get starting balance
    settings = db.get_settings()
    starting_balance = settings.get("starting_balance", 0)
    
    # Calculate current balance
    all_income = sum(t["amount"] for t in all_transactions if t["type"] == "Income")
    all_expense = sum(t["amount"] for t in all_transactions if t["type"] == "Expense")
    all_emi = sum(t["amount"] for t in all_transactions if t["type"] == "EMI")
    
    current_balance = starting_balance + all_income - all_expense - all_emi - total_debt_given + total_debt_received
    
    # Get EMI summary
    emis = db.get_emis("active")
    total_emi_remaining = sum(emi["remaining_amount"] for emi in emis)
    
    # Get debt summary
    debts = db.get_debts()
    money_to_receive = sum(d["remaining_amount"] for d in debts if d["type"] == "given")
    money_to_pay = sum(d["remaining_amount"] for d in debts if d["type"] == "received")
    
    return {
        "period": period,
        "period_income": total_income,
        "period_expense": total_expense,
        "current_balance": current_balance,
        "total_savings": total_savings_all,
        "money_to_receive": money_to_receive,
        "money_to_pay": money_to_pay,
        "active_emis": len(emis),
        "total_emi_remaining": total_emi_remaining,
        "starting_balance": starting_balance
    }

# EMI endpoints
@api_router.post("/emis")
async def create_emi(emi: EMICreate):
    emi_dict = {
        "name": emi.name,
        "total_amount": emi.total_amount,
        "paid_amount": 0,
        "remaining_amount": emi.total_amount,
        "monthly_emi": emi.monthly_emi,
        "start_date": emi.start_date,
        "end_date": emi.end_date,
        "status": "active",
        "created_at": format_timestamp()
    }
    
    result = db.add_emi(emi_dict)
    return result

@api_router.get("/emis")
async def get_emis(status: Optional[str] = None):
    emis = db.get_emis(status)
    return emis

# Debt endpoints
@api_router.get("/debts")
async def get_debts(type: Optional[str] = None):
    debts = db.get_debts(type)
    return debts

@api_router.post("/debts/repay")
async def repay_debt(person_name: str, amount: float, type: str):
    debts = db.get_debts(type)
    debt = next((d for d in debts if d['person_name'] == person_name), None)
    
    if not debt:
        raise HTTPException(status_code=404, detail="Debt not found")
    
    new_paid = debt["paid_amount"] + amount
    new_remaining = debt["remaining_amount"] - amount
    
    if new_remaining < 0:
        raise HTTPException(status_code=400, detail="Repayment amount exceeds remaining debt")
    
    db.update_debt(
        person_name=person_name,
        debt_type=type,
        updates={
            "paid_amount": new_paid,
            "remaining_amount": new_remaining,
            "updated_at": datetime.utcnow().isoformat()
        }
    )
    
    # Create a transaction for the repayment
    trans_type = "Income" if type == "given" else "Expense"
    transaction = {
        "timestamp": datetime.utcnow().isoformat(),
        "type": trans_type,
        "category": "Debt Repayment",
        "subcategory": f"From {person_name}" if type == "given" else f"To {person_name}",
        "amount": amount,
        "frequency": "One-time",
        "payment_mode": "Bank",
        "notes": f"Debt repayment - {person_name}",
        "month": datetime.utcnow().strftime("%B"),
        "year": datetime.utcnow().year,
        "debt_person": person_name,
        "emi_id": ""
    }
    
    db.add_transaction(transaction)
    
    return {"message": "Repayment recorded", "remaining_amount": new_remaining}

# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
