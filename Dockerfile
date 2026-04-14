FROM public.ecr.aws/docker/library/node:16

WORKDIR /app

COPY package*.json ./
COPY package*.json ./
COPY package*.json ./
COPY nest-cli.json ./
COPY tsconfig.build.json ./
COPY tsconfig.json ./
COPY package*.json ./

# Copy the RDS CA certificate to the container
COPY certificates/rds-ca-2019-root.pem /app/certificates/

# Install runtime dependecies (without dev/test dependecies)
RUN npm install

RUN npm install pm2 -g

RUN npm run build

# Copy production build
COPY . .

EXPOSE 3005


# Set the environment variable to point to the RDS CA certificate location
ENV RDS_CA_PATH=/app/certificates/rds-ca-2019-root.pem

CMD [ "pm2-runtime", "start", "npm", "--", "start" ]