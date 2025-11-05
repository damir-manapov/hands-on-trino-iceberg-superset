from superset.app import create_app


def ensure_trino_database():
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.models.core import Database

        name = "Trino (Iceberg)"
        uri = "trino://trino@trino:8080/iceberg"
        existing = db.session.query(Database).filter_by(database_name=name).one_or_none()
        if existing:
            print(f"[init] Superset database exists: {name}")
            return
        database = Database(database_name=name)
        database.set_sqlalchemy_uri(uri)
        db.session.add(database)
        db.session.commit()
        print(f"[init] Superset database added: {name}")


if __name__ == "__main__":
    ensure_trino_database()


