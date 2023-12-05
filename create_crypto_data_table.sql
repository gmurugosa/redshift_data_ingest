CREATE TABLE CryptoData (
    id INT IDENTITY(1,1) PRIMARY KEY,
    date DATE,
    coin VARCHAR(10),
    opening NUMERIC(18, 8),
    closing NUMERIC(18, 8),
    lowest NUMERIC(18, 8),
    highest NUMERIC(18, 8),
    volume VARCHAR(50),
    quantity VARCHAR(50),
    amount INT,
    avg_price NUMERIC(18, 8)
);