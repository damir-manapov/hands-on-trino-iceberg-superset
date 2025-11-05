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


def create_chart_for_dataset(dataset, chart_name, preferred_columns=None):
    """Create a table chart for a dataset"""
    from superset import db
    from superset.models.slice import Slice
    import json
    
    try:
        # Ensure columns are synced
        if not dataset.columns:
            dataset.fetch_metadata()
            db.session.commit()
        
        # Check if chart already exists
        existing = db.session.query(Slice).filter_by(
            datasource_id=dataset.id,
            datasource_type="table",
            slice_name=chart_name
        ).first()
        
        if existing:
            print(f"[init] Chart exists: {chart_name}")
            db.session.refresh(existing)
            return existing
        
        # Get available columns from the dataset
        available_columns = [col.column_name for col in dataset.columns]
        if not available_columns:
            all_columns = []
        else:
            if preferred_columns:
                all_columns = [col for col in preferred_columns if col in available_columns]
            else:
                all_columns = available_columns[:5]
            # If no preferred columns found, use first few available
            if not all_columns and available_columns:
                all_columns = available_columns[:5]
        
        # Create form_data for a simple table chart
        form_data = {
            "datasource": f"{dataset.id}__table",
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
            slice_name=chart_name,
            datasource_id=dataset.id,
            datasource_type="table",
            viz_type="table",
            params=json.dumps(form_data),
            description=f"Overview of {dataset.table_name} data",
        )
        db.session.add(chart)
        db.session.commit()
        print(f"[init] Chart created: {chart_name}")
        return chart
        
    except Exception as e:
        print(f"[init] Warning: Could not create chart {chart_name}: {e}")
        db.session.rollback()
        return None


def ensure_charts(datasets):
    """Create example charts for all datasets"""
    if not datasets:
        print("[init] No datasets available for chart creation")
        return []
    
    charts = []
    
    # Chart configurations for each table
    chart_configs = [
        ("events", "Events Overview", ["id", "ts", "event_type", "user_id", "amount", "payload"]),
        ("users", "Users Overview", ["user_id", "username", "email", "country", "active", "created_at"]),
        ("orders", "Orders Overview", ["order_id", "user_id", "product_id", "quantity", "price", "status", "order_date"]),
        ("products", "Products Overview", ["product_id", "name", "category", "price", "stock", "created_at"]),
        ("transactions", "Transactions Overview", ["transaction_id", "user_id", "amount", "currency", "transaction_type", "status", "timestamp"]),
    ]
    
    # Create charts for each dataset
    for table_name, chart_name, preferred_columns in chart_configs:
        dataset = next((d for d in datasets if d.table_name == table_name), None)
        if dataset:
            chart = create_chart_for_dataset(dataset, chart_name, preferred_columns)
            if chart:
                charts.append(chart)
        else:
            print(f"[init] Dataset not found for {table_name}, skipping chart")
    
    return charts


def ensure_dashboard(charts):
    """Create a dashboard with all charts"""
    from superset import db
    from superset.models.dashboard import Dashboard
    import json
    
    if not charts:
        print("[init] No charts available for dashboard creation")
        return
    
    try:
        # Check if dashboard already exists
        existing = db.session.query(Dashboard).filter_by(
            dashboard_title="Iceberg Demo Dashboard"
        ).first()
        
        if existing:
            print(f"[init] Dashboard exists: {existing.dashboard_title}")
            # Update existing dashboard with all charts
            updated = False
            for chart in charts:
                if chart not in existing.slices:
                    existing.slices.append(chart)
                    updated = True
            
            # Update JSON metadata to include all charts
            try:
                import json as json_module
                metadata = json_module.loads(existing.json_metadata or "{}")
                grid_children = metadata.get("GRID_ID", {}).get("children", [])
                
                # Add any missing charts to the layout
                for idx, chart in enumerate(charts, 1):
                    chart_id = f"CHART-{idx}"
                    if chart_id not in metadata:
                        metadata[chart_id] = {
                            "children": [],
                            "id": chart_id,
                            "meta": {
                                "chartId": chart.id,
                                "height": 50,
                                "sliceName": chart.slice_name,
                                "width": 6,
                            },
                            "type": "CHART",
                            "parents": ["ROOT_ID", "GRID_ID"],
                        }
                        if chart_id not in grid_children:
                            grid_children.append(chart_id)
                
                if "GRID_ID" not in metadata:
                    metadata["GRID_ID"] = {"children": [], "id": "GRID_ID", "parents": ["ROOT_ID"], "type": "GRID"}
                metadata["GRID_ID"]["children"] = grid_children
                existing.json_metadata = json_module.dumps(metadata)
                updated = True
            except Exception as e:
                print(f"[init] Warning: Could not update dashboard metadata: {e}")
            
            if updated:
                db.session.commit()
                print(f"[init] Dashboard updated with {len(charts)} charts")
            return
        
        # Create dashboard JSON metadata with all charts positioned in a grid
        # Arrange charts in rows, 2 per row (each 6 width)
        dashboard_json = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {
                "children": ["GRID_ID"],
                "id": "ROOT_ID",
                "type": "ROOT",
            },
            "GRID_ID": {
                "children": [],
                "id": "GRID_ID",
                "parents": ["ROOT_ID"],
                "type": "GRID",
            },
        }
        
        # Create chart components
        for idx, chart in enumerate(charts, 1):
            chart_id = f"CHART-{idx}"
            # Position: 2 charts per row, each taking 6 columns
            row = (idx - 1) // 2
            col = ((idx - 1) % 2) * 6
            
            dashboard_json[chart_id] = {
                "children": [],
                "id": chart_id,
                "meta": {
                    "chartId": chart.id,
                    "height": 50,
                    "sliceName": chart.slice_name,
                    "width": 6,
                },
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID"],
            }
            dashboard_json["GRID_ID"]["children"].append(chart_id)
        
        dashboard = Dashboard(
            dashboard_title="Iceberg Demo Dashboard",
            slug="iceberg-demo-dashboard",
            json_metadata=json.dumps(dashboard_json),
            published=True,
        )
        db.session.add(dashboard)
        db.session.commit()
        
        # Add all charts to dashboard
        for chart in charts:
            dashboard.slices.append(chart)
        db.session.commit()
        
        print(f"[init] Dashboard created: Iceberg Demo Dashboard with {len(charts)} charts")
        
    except Exception as e:
        print(f"[init] Warning: Could not create dashboard: {e}")
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
        # Create example charts for all datasets
        charts = []
        if datasets:
            charts = ensure_charts(datasets)
        # Create dashboard with all charts
        if charts:
            ensure_dashboard(charts)


