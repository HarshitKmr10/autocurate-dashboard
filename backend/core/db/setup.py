"""Database setup script for Prisma."""

import asyncio
import subprocess
import sys
import os
from pathlib import Path

# Add the backend directory to the Python path
backend_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_dir))


async def setup_database():
    """Setup database with Prisma."""
    print("🚀 Setting up database with Prisma...")
    
    # Get the directory containing the schema file
    schema_dir = Path(__file__).parent
    
    try:
        # Generate Prisma client
        print("📦 Generating Prisma client...")
        result = subprocess.run(
            ["prisma", "generate"],
            cwd=schema_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"❌ Failed to generate Prisma client: {result.stderr}")
            return False
        
        print("✅ Prisma client generated successfully")
        
        # Push schema to database (creates tables)
        print("🗄️  Pushing schema to database...")
        result = subprocess.run(
            ["prisma", "db", "push"],
            cwd=schema_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"❌ Failed to push schema: {result.stderr}")
            return False
        
        print("✅ Database schema pushed successfully")
        
        return True
        
    except FileNotFoundError:
        print("❌ Prisma CLI not found. Please install it first:")
        print("   npm install -g prisma")
        return False
    except Exception as e:
        print(f"❌ Error during database setup: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(setup_database())
    if success:
        print("🎉 Database setup completed successfully!")
    else:
        print("💥 Database setup failed!")
        sys.exit(1) 