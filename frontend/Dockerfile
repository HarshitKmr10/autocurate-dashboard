# Frontend Dockerfile for Autocurate Analytics Dashboard

FROM node:20-alpine

# Set working directory
WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm install

# Copy source code
COPY . .

# Set environment variables
ENV NODE_ENV=development
ENV NEXT_PUBLIC_API_URL=http://localhost:8000

# Expose port
EXPOSE 3000

# Start development server
CMD ["npm", "run", "dev"]