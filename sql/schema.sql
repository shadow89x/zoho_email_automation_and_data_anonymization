/*
Comprehensive Database Schema for Email-Customer Analytics Pipeline

What: Complete relational database schema for optical retail customer relationship management
Why: Enables structured storage, querying, and analysis of customer, inventory, sales, and email data
How: Normalized design with proper foreign keys, constraints, and indexes for performance
Alternative: Could use NoSQL (MongoDB) for flexibility, but relational structure fits business data better

Schema Design Principles:
- Normalized structure to reduce data redundancy
- Foreign key constraints to maintain referential integrity
- Appropriate data types for storage efficiency and query performance
- Extensible design to accommodate future business requirements

Package Selection Rationale:
- PostgreSQL: Advanced features, JSON support, excellent performance
- Alternative: MySQL (simpler) or SQL Server (enterprise), but PostgreSQL offers best balance
*/

-- =====================================================
-- CUSTOMERS TABLE
-- =====================================================
/*
What: Central customer information repository with contact details and account management
Why: Single source of truth for customer data across all business operations
How: Stores essential customer info with unique identifiers and contact methods
Alternative: Could split into separate contact and account tables, but consolidated is simpler
*/
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,                    -- Auto-incrementing unique identifier
    customer_name TEXT NOT NULL,                       -- Business/customer name (required)
    account_no TEXT UNIQUE,                           -- External account number (unique business constraint)
    business_id TEXT,                                  -- Internal business identifier for grouping
    email TEXT,                                        -- Primary email contact
    cc_email TEXT,                                     -- Secondary/CC email contact
    phone TEXT,                                        -- Primary phone number
    fax TEXT,                                          -- Fax number (legacy but still used in optical industry)
    primary_contact TEXT,                              -- Main contact person name
    account_type TEXT DEFAULT 'Lens',                 -- Account classification (Lens, Frame, Accessory, etc.)
    is_active BOOLEAN DEFAULT TRUE,                   -- Account status flag
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,   -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP    -- Last modification timestamp
);

-- Create indexes for performance optimization
-- Why these indexes: Common query patterns for customer lookup and business analysis
CREATE INDEX idx_customers_business_id ON customers(business_id);
CREATE INDEX idx_customers_account_no ON customers(account_no);
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_account_type ON customers(account_type);

-- =====================================================
-- INVENTORY/ITEMS TABLE  
-- =====================================================
/*
What: Product catalog with inventory tracking and vendor management
Why: Centralizes product information for sales analysis and inventory management
How: Stores item details with quantity tracking and vendor relationships
Alternative: Could separate into products and inventory tables, but combined is more efficient
*/
CREATE TABLE items (
    item_id SERIAL PRIMARY KEY,                       -- Auto-incrementing unique identifier
    item_code TEXT UNIQUE NOT NULL,                   -- Unique product code (business key)
    description TEXT,                                  -- Product description
    category TEXT,                                     -- Product category classification
    preferred_vendor TEXT,                             -- Primary supplier information
    quantity_on_hand INTEGER DEFAULT 0,               -- Current inventory count
    physical_count INTEGER,                            -- Last physical inventory count
    unit_cost NUMERIC(10,2),                          -- Cost per unit for margin analysis
    list_price NUMERIC(10,2),                         -- Standard selling price
    is_active BOOLEAN DEFAULT TRUE,                   -- Product status (active/discontinued)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,   -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP    -- Last modification timestamp
);

-- Create indexes for inventory management and sales analysis
-- Why these indexes: Support product lookup, category analysis, and inventory reporting
CREATE INDEX idx_items_item_code ON items(item_code);
CREATE INDEX idx_items_category ON items(category);
CREATE INDEX idx_items_vendor ON items(preferred_vendor);
CREATE INDEX idx_items_active ON items(is_active);

-- =====================================================
-- ORDERS/SALES TABLE
-- =====================================================
/*
What: Sales transaction records linking customers to products with pricing and timing
Why: Enables sales analysis, customer behavior tracking, and revenue reporting
How: Stores transactional data with foreign key relationships to customers and items
Alternative: Could separate orders and order_items, but single table is sufficient for this scale
*/
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,                      -- Auto-incrementing unique identifier
    order_date DATE NOT NULL,                         -- Transaction date (required for reporting)
    customer_name TEXT,                                -- Customer identifier (denormalized for performance)
    business_id TEXT,                                  -- Business grouping identifier
    item_code TEXT,                                    -- Product identifier
    quantity INTEGER NOT NULL DEFAULT 1,              -- Quantity ordered
    sales_price NUMERIC(10,2),                        -- Actual selling price (may differ from list)
    amount NUMERIC(10,2),                             -- Total line amount (quantity * sales_price)
    memo TEXT,                                         -- Order notes or special instructions
    order_type TEXT DEFAULT 'Standard',               -- Order classification
    status TEXT DEFAULT 'Completed',                  -- Order status tracking
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,   -- Record creation timestamp
    
    -- Foreign key constraints for referential integrity
    -- Why foreign keys: Ensures data consistency and enables efficient joins
    FOREIGN KEY (item_code) REFERENCES items(item_code) ON UPDATE CASCADE,
    FOREIGN KEY (business_id) REFERENCES customers(business_id) ON UPDATE CASCADE
);

-- Create indexes for sales analysis and reporting
-- Why these indexes: Support date-based reporting, customer analysis, and product performance
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_customer_name ON orders(customer_name);
CREATE INDEX idx_orders_business_id ON orders(business_id);
CREATE INDEX idx_orders_item_code ON orders(item_code);
CREATE INDEX idx_orders_amount ON orders(amount);

-- =====================================================
-- EMAILS TABLE
-- =====================================================
/*
What: Email communication repository with metadata and content for customer relationship analysis
Why: Enables communication tracking, customer service analysis, and relationship insights
How: Stores email metadata and content with privacy considerations and search optimization
Alternative: Could use dedicated email systems, but integrated approach enables cross-analysis
*/
CREATE TABLE emails (
    email_id SERIAL PRIMARY KEY,                      -- Auto-incrementing unique identifier
    subject TEXT,                                      -- Email subject line
    summary TEXT,                                      -- Email body content or summary
    from_address TEXT,                                 -- Sender email address
    to_address TEXT,                                   -- Primary recipient email address
    cc_address TEXT,                                   -- CC recipients (comma-separated)
    bcc_address TEXT,                                  -- BCC recipients (comma-separated)
    received_time TIMESTAMP,                           -- Email receipt timestamp
    sent_time TIMESTAMP,                               -- Email send timestamp
    message_id TEXT UNIQUE,                           -- Unique email message identifier
    has_attachment BOOLEAN DEFAULT FALSE,             -- Attachment indicator
    attachment_count INTEGER DEFAULT 0,               -- Number of attachments
    folder_id TEXT,                                    -- Email folder/label classification
    thread_id TEXT,                                    -- Conversation thread identifier
    priority INTEGER DEFAULT 3,                       -- Email priority (1=high, 3=normal, 5=low)
    size_bytes INTEGER,                                -- Email size for storage analysis
    business_id TEXT,                                  -- Linked business identifier
    customer_match_score NUMERIC(5,2),               -- Customer matching confidence score
    is_anonymized BOOLEAN DEFAULT FALSE,              -- Privacy processing indicator
    raw_json JSONB,                                    -- Original email metadata (JSON format)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,   -- Record creation timestamp
    
    -- Foreign key constraint for business relationship
    FOREIGN KEY (business_id) REFERENCES customers(business_id) ON UPDATE CASCADE
);

-- Create indexes for email analysis and search
-- Why these indexes: Support email search, customer communication analysis, and thread tracking
CREATE INDEX idx_emails_from_address ON emails(from_address);
CREATE INDEX idx_emails_to_address ON emails(to_address);
CREATE INDEX idx_emails_received_time ON emails(received_time);
CREATE INDEX idx_emails_sent_time ON emails(sent_time);
CREATE INDEX idx_emails_business_id ON emails(business_id);
CREATE INDEX idx_emails_thread_id ON emails(thread_id);
CREATE INDEX idx_emails_subject ON emails USING gin(to_tsvector('english', subject));  -- Full-text search
CREATE INDEX idx_emails_summary ON emails USING gin(to_tsvector('english', summary));  -- Full-text search

-- =====================================================
-- BUSINESS ANALYTICS VIEWS
-- =====================================================
/*
What: Pre-computed views for common business intelligence queries
Why: Improves query performance and provides consistent business metrics
How: Creates materialized views that can be refreshed periodically
Alternative: Could compute on-demand, but views provide better performance for dashboards
*/

-- Customer communication summary view
CREATE VIEW customer_communication_summary AS
SELECT 
    c.business_id,
    c.customer_name,
    c.account_type,
    COUNT(e.email_id) as total_emails,
    MAX(e.received_time) as last_email_date,
    MIN(e.received_time) as first_email_date,
    AVG(e.customer_match_score) as avg_match_score,
    SUM(CASE WHEN e.has_attachment THEN 1 ELSE 0 END) as emails_with_attachments
FROM customers c
LEFT JOIN emails e ON c.business_id = e.business_id
WHERE c.is_active = TRUE
GROUP BY c.business_id, c.customer_name, c.account_type;

-- Sales performance summary view  
CREATE VIEW sales_performance_summary AS
SELECT 
    c.business_id,
    c.customer_name,
    c.account_type,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_revenue,
    AVG(o.amount) as avg_order_value,
    MAX(o.order_date) as last_order_date,
    MIN(o.order_date) as first_order_date
FROM customers c
LEFT JOIN orders o ON c.business_id = o.business_id
WHERE c.is_active = TRUE
GROUP BY c.business_id, c.customer_name, c.account_type;

-- Product performance summary view
CREATE VIEW product_performance_summary AS
SELECT 
    i.item_code,
    i.description,
    i.category,
    COUNT(o.order_id) as total_orders,
    SUM(o.quantity) as total_quantity_sold,
    SUM(o.amount) as total_revenue,
    AVG(o.sales_price) as avg_selling_price,
    MAX(o.order_date) as last_sale_date
FROM items i
LEFT JOIN orders o ON i.item_code = o.item_code
WHERE i.is_active = TRUE
GROUP BY i.item_code, i.description, i.category;

-- =====================================================
-- TRIGGERS AND FUNCTIONS
-- =====================================================
/*
What: Automated database maintenance and business logic enforcement
Why: Ensures data consistency and automates routine maintenance tasks
How: Uses PostgreSQL triggers and functions for real-time processing
Alternative: Could handle in application code, but database triggers are more reliable
*/

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers to automatically update timestamps
CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_items_updated_at BEFORE UPDATE ON items
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to automatically calculate order amount
CREATE OR REPLACE FUNCTION calculate_order_amount()
RETURNS TRIGGER AS $$
BEGIN
    -- Automatically calculate amount if not provided
    IF NEW.amount IS NULL THEN
        NEW.amount = NEW.quantity * COALESCE(NEW.sales_price, 0);
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to calculate order amounts
CREATE TRIGGER calculate_order_amount_trigger BEFORE INSERT OR UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION calculate_order_amount();

-- =====================================================
-- SECURITY AND PERMISSIONS
-- =====================================================
/*
What: Database security configuration for multi-user access
Why: Protects sensitive customer and business data with role-based access
How: Creates roles with specific permissions for different user types
Alternative: Could use application-level security, but database-level is more secure
*/

-- Create roles for different access levels
-- CREATE ROLE email_analytics_readonly;
-- CREATE ROLE email_analytics_readwrite;
-- CREATE ROLE email_analytics_admin;

-- Grant appropriate permissions
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO email_analytics_readonly;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO email_analytics_readwrite;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO email_analytics_admin;

-- =====================================================
-- INITIAL DATA AND CONSTRAINTS
-- =====================================================
/*
What: Data validation rules and initial reference data
Why: Ensures data quality and provides consistent business rules
How: Uses check constraints and reference data inserts
Alternative: Could validate in application, but database constraints are more reliable
*/

-- Add check constraints for data validation
ALTER TABLE customers ADD CONSTRAINT chk_customer_name_not_empty 
    CHECK (LENGTH(TRIM(customer_name)) > 0);

ALTER TABLE orders ADD CONSTRAINT chk_quantity_positive 
    CHECK (quantity > 0);

ALTER TABLE orders ADD CONSTRAINT chk_amount_non_negative 
    CHECK (amount >= 0);

ALTER TABLE emails ADD CONSTRAINT chk_priority_range 
    CHECK (priority BETWEEN 1 AND 5);

-- Comments for documentation
COMMENT ON TABLE customers IS 'Central customer repository with contact information and business relationships';
COMMENT ON TABLE items IS 'Product catalog with inventory tracking and vendor management';
COMMENT ON TABLE orders IS 'Sales transaction records linking customers to products';
COMMENT ON TABLE emails IS 'Email communication repository for customer relationship analysis';

COMMENT ON COLUMN customers.business_id IS 'Internal identifier for grouping related customer accounts';
COMMENT ON COLUMN orders.amount IS 'Total line amount calculated as quantity * sales_price';
COMMENT ON COLUMN emails.customer_match_score IS 'Confidence score for customer-email matching algorithm';
COMMENT ON COLUMN emails.raw_json IS 'Original email metadata preserved for audit and analysis';
