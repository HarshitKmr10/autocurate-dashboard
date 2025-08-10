# Autocurate Analytics Dashboard

AI-powered analytics dashboard generator that transforms CSV data into intelligent, domain-aware visualizations.

## 🎯 Overview

Autocurate is an end-to-end system that takes any CSV file, profiles the data, detects the business domain, and automatically generates a fully dynamic, domain-aware analytics dashboard with relevant KPIs, charts, and filters. **No hardcoded templates or manual configuration required.**

## ✨ Key Features

### 🤖 AI-Powered Domain Detection
- Automatically detects business context (e-commerce, finance, manufacturing, SaaS, etc.)
- Uses both rule-based analysis and LLM classification with GPT-4.1 Mini
- Provides confidence scores and detailed explanations

### 📊 Dynamic Dashboard Generation
- **Zero Configuration**: No hardcoded templates or manual setup
- **Contextually Relevant**: Generates KPIs and visualizations specific to your domain
- **Adaptive Layouts**: Smart positioning based on data characteristics and importance
- **Explanation Layer**: Shows why each KPI and chart was selected

### ⚡ Real-time Interactivity
- Smart filters that update all dashboard components
- Interactive charts with drill-down capabilities
- Responsive design optimized for all screen sizes
- Optional natural language queries for custom visualizations

### 🚀 Performance Optimized
- **Fast Processing**: <15s dashboard generation for typical CSV files
- **Efficient Storage**: DuckDB for analytics, Redis for caching
- **Modern Stack**: React, FastAPI, and optimized rendering

## 🛠️ Tech Stack

### Backend (Python FastAPI)
- **FastAPI**: High-performance async API framework
- **Prisma ORM**: Modern Python ORM with type safety
- **Supabase**: PostgreSQL database with real-time features
- **DuckDB**: Analytics database for fast CSV processing
- **Redis**: Caching and real-time updates
- **Pandas/Polars**: Data manipulation and analysis
- **LangChain**: LLM orchestration and prompt management

### Frontend (Next.js TypeScript)
- **Next.js 14**: Full-stack React framework with App Router
- **TypeScript**: Type safety and developer experience
- **Tailwind CSS**: Utility-first styling
- **Plotly.js**: Advanced interactive charts
- **Recharts**: React chart library
- **SWR**: Data fetching and caching

### AI & Analytics
- **OpenAI GPT-4.1 Mini**: Cost-effective LLM for domain detection and curation
- **Scikit-learn**: Statistical analysis and ML
- **NumPy/SciPy**: Numerical computing

### Development & Deployment
- **pyenv**: Python environment management
- **Vercel**: Frontend deployment and hosting
- **AWS/Google Cloud**: Backend deployment with Docker
- **Docker**: Containerization for backend services

## 🚀 Quick Start

### Prerequisites
- Python 3.11+ with pyenv
- Node.js 18+
- OpenAI API key
- Docker (optional, for full stack)

### Option 1: Local Development

1. **Clone and Setup Environment**
   ```bash
   git clone <repository-url>
   cd autocurate-dashboard
   
   # Setup Python environment
   pyenv install 3.12.0
   pyenv virtualenv 3.12.0 autocurate-dashboard
   pyenv activate autocurate-dashboard
   ```

2. **Backend Setup**
   ```bash
   cd backend
   pip install -r requirements.txt
   
   # Copy environment template and add your OpenAI API key
   cp env.example .env
   # Edit .env file with your settings
   ```

3. **Frontend Setup**
   ```bash
   cd ../frontend
   npm install
   ```

4. **Start Services**
   ```bash
   # Terminal 1: Backend
   cd backend
   uvicorn backend.main:app --reload
   
   # Terminal 2: Frontend
   cd frontend
   npm run dev
   ```

5. **Access Application**
   - Frontend: http://localhost:3000
   - Backend API: http://localhost:8000
   - API Docs: http://localhost:8000/api/docs

### Option 2: Docker Compose

1. **Quick Start**
   ```bash
   git clone <repository-url>
   cd autocurate-dashboard
   
   # Set your OpenAI API key
   export OPENAI_API_KEY=your-api-key-here
   
   # Start all services
   docker-compose up -d
   ```

2. **Access Application**
   - Frontend: http://localhost:3000
   - Backend: http://localhost:8000

## 📋 Usage Guide

### 1. Upload Your CSV File
- **Drag & Drop**: Simply drag your CSV file onto the upload area
- **File Browser**: Click to browse and select your file
- **Validation**: Real-time validation with detailed feedback
- **Size Limit**: Up to 15MB files supported

### 2. Automatic Processing
The system automatically:
- **Profiles** your data (types, statistics, patterns)
- **Detects** business domain using AI
- **Generates** contextual KPIs and visualizations
- **Optimizes** layout based on data importance

### 3. Explore Your Dashboard
- **Interactive KPIs**: Domain-specific key metrics
- **Smart Charts**: Automatically selected chart types
- **Dynamic Filters**: Filter data across all components
- **Responsive Design**: Works on desktop, tablet, and mobile

## 🎯 Supported Business Domains

| Domain | KPIs | Chart Types | Sample Data |
|--------|------|-------------|-------------|
| **E-commerce** | Revenue, Orders, AOV, Conversion | Time series, Funnels, Category breakdown | Orders, Products, Customers |
| **Finance** | Total Assets, ROI, Risk Metrics | Portfolio charts, Risk analysis | Transactions, Accounts, Investments |
| **Manufacturing** | OEE, Defect Rate, Throughput | Production timelines, Quality control | Production data, Quality metrics |
| **SaaS** | MRR, Churn Rate, DAU/MAU | Cohort analysis, User funnels | Users, Subscriptions, Usage |
| **Generic** | Count, Sum, Average | Standard analytics | Any structured data |

## 🔧 Configuration

### Environment Variables

**Backend (.env)**
```bash
# Required
OPENAI_API_KEY=your-openai-api-key

# Optional (with defaults)
DATABASE_URL=postgresql://user:pass@localhost:5432/autocurate
REDIS_URL=redis://localhost:6379/0
MAX_FILE_SIZE=15728640
DEFAULT_SAMPLE_SIZE=1000
```

**Frontend (.env.local)**
```bash
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_MAX_FILE_SIZE=15728640
```

## 📚 API Documentation

### Core Endpoints

**File Upload & Processing**
```bash
POST /api/v1/upload              # Upload CSV file
GET  /api/v1/upload/{id}/status  # Check processing status
```

**Dashboard Generation**
```bash
GET /api/v1/dashboard/{id}         # Get dashboard config
GET /api/v1/dashboard/{id}/preview # Get with sample data
```

**Analytics & Queries**
```bash
POST /api/v1/analytics/{id}/query   # Execute SQL queries
GET  /api/v1/analytics/{id}/profile # Get data profile
```

**Natural Language**
```bash
POST /api/v1/nl/{id}/query # Parse natural language queries
```

## 🧪 Testing

### Backend Tests
```bash
cd backend
pytest
pytest --cov=backend tests/  # With coverage
```

### Frontend Tests
```bash
cd frontend
npm test
npm run test:e2e  # End-to-end tests
```

## 🚀 Deployment

### Frontend (Vercel)
1. **Connect Repository** to Vercel
2. **Set Environment Variables**:
   - `NEXT_PUBLIC_API_URL`: Your backend URL
3. **Auto-deploy** on main branch push

### Backend (AWS/Google Cloud)
1. **Build Production Image**:
   ```bash
   docker build -f backend/Dockerfile.prod -t autocurate-backend .
   ```
2. **Deploy** to your cloud provider
3. **Configure** environment variables and secrets

## 🤝 Contributing

1. **Fork** the repository
2. **Create** feature branch: `git checkout -b feature/amazing-feature`
3. **Commit** changes: `git commit -m 'Add amazing feature'`
4. **Push** to branch: `git push origin feature/amazing-feature`
5. **Open** Pull Request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- **OpenAI** for GPT-4.1 Mini API
- **Plotly** for powerful visualization capabilities
- **Supabase** for seamless database integration
- **Vercel** for excellent deployment experience
- The open-source community for amazing tools and libraries

---

**Made with ❤️ by the Autocurate Team**