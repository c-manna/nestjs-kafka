# Step 1: Use Node.js as base image
FROM node:16

# Step 2: Set working directory
WORKDIR /usr/src/app

# Step 3: Install dependencies
COPY package*.json ./
RUN npm install

# Step 4: Copy project files
COPY . .

# Step 5: Expose port
EXPOSE 3000

# Step 6: Start the app
CMD ["npm", "run", "start"]
