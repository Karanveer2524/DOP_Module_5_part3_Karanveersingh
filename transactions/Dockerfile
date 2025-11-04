FROM node:18-alpine

WORKDIR /app
COPY src/package.json .
RUN npm install
COPY src .

# Ensure this matches your app's port
EXPOSE 3000  
CMD ["npm", "start"]