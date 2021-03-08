FROM node:8

WORKDIR /usr/src/app
COPY package*.json ./
RUN apt-get update && apt-get install net-tools
RUN npm install
COPY . .
RUN mkdir log
EXPOSE 3000
CMD [ "npm", "start" ]