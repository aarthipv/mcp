from mcp.server.fastmcp import FastMCP
from typing import List


# PostgreSQL integration
import psycopg2

def get_connection():
    # Replace the placeholders with your actual database credentials
    return psycopg2.connect(
        host="localhost",
        dbname="aarthiprashanth",
        user="aarthiprashanth",
        password=""
    )

# Create MCP server
mcp = FastMCP("LeaveManager")

# Tool: Check Leave Balance
@mcp.tool()
def get_leave_balance(employee_id: str) -> str:
    """Check how many leave days are left for the employee"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT balance FROM employee_leaves WHERE emp_id = %s", (employee_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return f"{employee_id} has {row[0]} leave days remaining."
        return "Employee ID not found."
    except Exception as e:
        return f"Database error: {e}"

# Tool: Apply for Leave with specific dates
@mcp.tool()
def apply_leave(employee_id: str, leave_dates: List[str]) -> str:
    """
    Apply leave for specific dates (e.g., ["2025-04-17", "2025-05-01"])
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        # Check current balance
        cur.execute("SELECT balance FROM employee_leaves WHERE emp_id = %s", (employee_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return "Employee ID not found."
        available_balance = row[0]
        requested_days = len(leave_dates)
        if available_balance < requested_days:
            cur.close()
            conn.close()
            return f"Insufficient leave balance. You requested {requested_days} day(s) but have only {available_balance}."
        # Deduct balance
        new_balance = available_balance - requested_days
        cur.execute("UPDATE employee_leaves SET balance = %s WHERE emp_id = %s", (new_balance, employee_id))
        # Insert leave dates into leave_history
        for date in leave_dates:
            cur.execute(
                "INSERT INTO leave_history (emp_id, leave_date) VALUES (%s, %s)",
                (employee_id, date)
            )
        conn.commit()
        cur.close()
        conn.close()
        return f"Leave applied for {requested_days} day(s). Remaining balance: {new_balance}."
    except Exception as e:
        return f"Database error: {e}"


# Resource: Leave history
@mcp.tool()
def get_leave_history(employee_id: str) -> str:
    """Get leave history for the employee"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT leave_date FROM leave_history WHERE emp_id = %s ORDER BY leave_date", (employee_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if rows:
            history = ', '.join(row[0].strftime("%Y-%m-%d") for row in rows)
            return f"Leave history for {employee_id}: {history}"
        else:
            # Check if employee exists
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM employee_leaves WHERE emp_id = %s", (employee_id,))
            exists = cur.fetchone()
            cur.close()
            conn.close()
            if exists:
                return f"Leave history for {employee_id}: No leaves taken."
            else:
                return "Employee ID not found."
    except Exception as e:
        return f"Database error: {e}"

# Resource: Greeting
@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Get a personalized greeting"""
    return f"Hello, {name}! How can I assist you with leave management today?"

if __name__ == "__main__":
    mcp.run()