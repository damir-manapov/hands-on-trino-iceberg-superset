from superset.app import create_app


def ensure_trino_database():
    from superset import db
    from superset.models.core import Database

    name = "Trino (Iceberg)"
    uri = "trino://trino@trino:8080/iceberg"
    existing = db.session.query(Database).filter_by(database_name=name).one_or_none()
    if existing:
        print(f"[init] Superset database exists: {name}")
        database = existing
    else:
        database = Database(database_name=name)
        database.set_sqlalchemy_uri(uri)
        db.session.add(database)
        db.session.commit()
        print(f"[init] Superset database added: {name}")
    
    return database


def ensure_datasets(database):
    """Create datasets for all seeded tables"""
    from superset import db
    from superset.connectors.sqla.models import SqlaTable
    
    tables = [
        ("events", "Events", "Event data with user interactions"),
        ("users", "Users", "User accounts and profiles"),
        ("orders", "Orders", "Order records with product and user information"),
        ("products", "Products", "Product catalog"),
        ("transactions", "Transactions", "Financial transaction records"),
    ]
    
    for table_name, label, description in tables:
        dataset_name = f"{label} (demo.{table_name})"
        
        try:
            # Check if dataset already exists
            existing = db.session.query(SqlaTable).filter_by(
                database_id=database.id,
                table_name=table_name,
                schema="demo"
            ).one_or_none()
            
            if existing:
                print(f"[init] Dataset exists: {dataset_name}")
                continue
            
            # Verify table exists in database
            from sqlalchemy import inspect as sql_inspect
            with database.get_sqla_engine() as engine:
                inspector = sql_inspect(engine)
                try:
                    columns = inspector.get_columns(table_name, schema="demo")
                except Exception as e:
                    print(f"[init] Warning: Table {table_name} not found in database: {e}")
                    continue
            
            # Create dataset
            dataset = SqlaTable(
                database_id=database.id,
                schema="demo",
                table_name=table_name,
            )
            # Set label and description after creation
            dataset.label = label
            dataset.description = description
            db.session.add(dataset)
            db.session.commit()
            print(f"[init] Dataset created: {dataset_name}")
            
        except Exception as e:
            print(f"[init] Warning: Could not create dataset {dataset_name}: {e}")
            db.session.rollback()
            continue


if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        database = ensure_trino_database()
        # Wait a moment for tables to be available
        import time
        time.sleep(2)
        ensure_datasets(database)


