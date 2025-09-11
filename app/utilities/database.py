from datetime import datetime, timezone

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    BigInteger,
    String,
    DateTime,
    ForeignKey,
)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship

from app.config import DATABASE_CONNECTION_STRING, appTimezone

# Create an engine
engine = create_engine(DATABASE_CONNECTION_STRING)

# Define a base class for declarative class definitions
Base = declarative_base()


# Define the BatchJobs class mapped to the BatchJobs table
class BatchJobs(Base):
    __tablename__ = "BatchJobs"

    id = Column(Integer, primary_key=True)
    uid = Column(String(120), unique=True)
    user_id = Column(Integer, ForeignKey("Users.id"))
    user = relationship("Users", backref="batch_jobs")
    original_file_name = Column(String(120), nullable=False)
    uploaded_file = Column(String(120), nullable=False)
    accepted_file = Column(String(), nullable=True)
    row_count = Column(Integer)
    email_column = Column(String(120))
    header_row = Column(Integer, nullable=False)
    source = Column(String(120), nullable=False, default="web")
    status = Column(String(120), nullable=False, default="pending_start")
    uploaded = Column(
        DateTime(),
        nullable=False,
        default=datetime.now(timezone.utc).astimezone(appTimezone),
    )
    started = Column(
        DateTime(),
        nullable=True,
    )


class Users(Base):
    __tablename__ = "Users"

    id = Column(Integer, primary_key=True)
    credits = Column(BigInteger)

    def save(self):
        # inject self into db session
        session.add(self)

        # commit change and save the object
        session.commit()

        return self


# Create a session
Session = sessionmaker(bind=engine)
session = Session()


def update_job_status(file, **kwargs):
    job = session.query(BatchJobs).filter_by(accepted_file=file).first()
    for key, value in kwargs.items():
        setattr(job, key, value)
    session.commit()


def file_has_a_job_in_db(file):
    return session.query(BatchJobs).filter_by(accepted_file=file).first() is not None


def get_job_status(file):
    job = session.query(BatchJobs).filter_by(accepted_file=file).first()
    return job.status


def set_job_status(file, status):
    job = session.query(BatchJobs).filter_by(accepted_file=file).first()
    job.status = status
    session.commit()
