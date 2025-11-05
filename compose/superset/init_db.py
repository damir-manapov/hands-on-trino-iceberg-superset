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
    
    datasets = []
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
                # Ensure columns are synced for existing datasets too
                try:
                    if not existing.columns:
                        existing.fetch_metadata()
                        db.session.commit()
                        print(f"[init] Synced columns for existing dataset: {dataset_name}")
                except Exception as e:
                    print(f"[init] Warning: Could not sync columns for existing dataset: {e}")
                datasets.append(existing)
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
            
            # Sync columns/metadata for the dataset
            try:
                dataset.fetch_metadata()
                db.session.commit()
                print(f"[init] Dataset created and synced: {dataset_name}")
            except Exception as e:
                print(f"[init] Dataset created but column sync failed: {e}")
            
            datasets.append(dataset)
            
        except Exception as e:
            print(f"[init] Warning: Could not create dataset {dataset_name}: {e}")
            db.session.rollback()
            continue
    
    return datasets


def ensure_charts(datasets):
    """Create example charts for datasets"""
    from superset import db
    from superset.models.slice import Slice
    import json
    
    # Only create charts if we have datasets
    if not datasets:
        print("[init] No datasets available for chart creation")
        return
    
    # Create a simple table chart showing row counts
    try:
        # Get the events dataset for the example chart
        events_dataset = next((d for d in datasets if d.table_name == "events"), None)
        if not events_dataset:
            print("[init] Events dataset not found, skipping chart creation")
            return
        
        # Ensure columns are synced
        try:
            if not events_dataset.columns:
                events_dataset.fetch_metadata()
                db.session.commit()
                print(f"[init] Synced columns for events dataset")
        except Exception as e:
            print(f"[init] Warning: Could not sync columns: {e}")
        
        # Check if chart already exists - if so, delete it to recreate with correct columns
        existing = db.session.query(Slice).filter_by(
            datasource_id=events_dataset.id,
            datasource_type="table"
        ).first()
        
        if existing:
            db.session.delete(existing)
            db.session.commit()
            print(f"[init] Deleted existing chart, recreating: Events Overview")
        
        # Get available columns from the dataset
        available_columns = [col.column_name for col in events_dataset.columns]
        if not available_columns:
            # If no columns synced, use a simpler chart with just count
            all_columns = []
        else:
            # Use available columns, preferring common ones
            preferred_columns = ["id", "ts", "event_type", "user_id", "amount", "payload"]
            all_columns = [col for col in preferred_columns if col in available_columns]
            # If no preferred columns found, use first few available
            if not all_columns and available_columns:
                all_columns = available_columns[:5]
        
        # Create form_data for a simple table chart
        form_data = {
            "datasource": f"{events_dataset.id}__table",
            "viz_type": "table",
            "slice_id": None,
            "url_params": {},
            "granularity_sqla": None,
            "time_grain_sqla": None,
            "time_range": "No filter",
            "query_mode": "raw",
            "groupby": [],
            "metrics": ["count"],
            "all_columns": all_columns,
            "percent_metrics": [],
            "order_by": [["count", False]],
            "row_limit": 100,
            "include_search": True,
            "page_length": 25,
        }
        
        chart = Slice(
            slice_name="Events Overview",
            datasource_id=events_dataset.id,
            datasource_type="table",
            viz_type="table",
            params=json.dumps(form_data),
            description="Overview of event data",
        )
        db.session.add(chart)
        db.session.commit()
        print(f"[init] Chart created: Events Overview")
        
    except Exception as e:
        print(f"[init] Warning: Could not create chart: {e}")
        import traceback
        traceback.print_exc()
        db.session.rollback()


if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        database = ensure_trino_database()
        # Wait a moment for tables to be available
        import time
        time.sleep(2)
        datasets = ensure_datasets(database)
        # Create example charts
        if datasets:
            ensure_charts(datasets)


