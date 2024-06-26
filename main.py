from sqlalchemy import *
from sqlalchemy.orm import *
from fastapi import FastAPI

app = FastAPI()

engine = create_engine("mysql://isp_p_Ignasheva:12345@77.91.86.135/isp_p_Ignasheva")

class Base(DeclarativeBase): pass

class Doctor(Base):
    __tablename__ = 'doctor'
    id = Column(Integer, primary_key=True, autoincrement=True)
    full_name = Column(String(100), nullable=False)
    specialization = Column(String(50), nullable=False)
    category = Column(Integer, nullable=False)
    appointments = relationship("Appointment", back_populates="doctor")

class Patient(Base):
    __tablename__ = 'patient'
    id = Column(Integer, primary_key=True, autoincrement=True)
    medical_card_number = Column(String(20), unique=True, nullable=False)
    full_name = Column(String(100), nullable=False)
    date_of_birth = Column(Date, nullable=False)
    address = Column(String(255), nullable=False)
    gender = Column(String(10), nullable=False)
    discount = Column(Float, nullable=False)
    appointments = relationship("Appointment", back_populates="patient")

class Appointment(Base):
    __tablename__ = 'appointment'
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticket_number = Column(String(20), nullable=False)
    date_of_visit = Column(DateTime, nullable=False)
    purpose = Column(String(50), nullable=False)
    cost = Column(Float, nullable=False)
    doctor_id = Column(Integer, ForeignKey('doctor.id'), nullable=False)
    patient_id = Column(Integer, ForeignKey('patient.id'), nullable=False)
    diagnosis_id = Column(Integer, ForeignKey('diagnosis.id'))
    doctor = relationship("Doctor", back_populates="appointments")
    patient = relationship("Patient", back_populates="appointments")
    diagnosis = relationship("Diagnosis", backref="appointments")

class Diagnosis(Base):
    __tablename__ = 'diagnosis'
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(20), unique=True, nullable=False)
    name = Column(String(100), nullable=False)

Base.metadata.create_all(engine)

@app.get('/doctors')
def get_doctors():
    with Session(autoflush=False, bind=engine) as db:
        doctors = db.query(Doctor).all()
        response = []
        for doctor in doctors:
            response.append(doctor)
    return {'doctors': response}
@app.get('/discount_patients')
def get_discount_patients():
    with Session(autoflush=False, bind=engine) as db:
        count = db.query(func.count(Patient.id)).filter(Patient.discount > 0).scalar()
    return {'count': count}
@app.get('/appointments/start={start_date}&end={end_date}')
def get_appointments_by_period(start_date: str, end_date: str):
    with Session(autoflush=False, bind=engine) as db:
        appointments = db.query(Appointment).filter(Appointment.date_of_visit.between(start_date, end_date)).all()
        response = []
        for appointment in appointments:
            response.append(appointment)
    return {'appointments': response}
@app.get('/revenue_month/{month}')
def get_revenue_by_month(month: int):
    with Session(autoflush=False, bind=engine) as db:
        revenue = db.query(func.sum(Appointment.cost)).filter(extract('month', Appointment.date_of_visit) == month).scalar()
    return {'revenue': revenue}
@app.get('/patients_diagnosis/{diagnosis_id}')
def get_patients_by_diagnosis(diagnosis_id: int):
    with Session(autoflush=False, bind=engine) as db:
        patients = db.query(Patient).join(Appointment).filter(Appointment.diagnosis_id == diagnosis_id).distinct().all()
        response = []
        for patient in patients:
            response.append(patient)
    return {'patients': response}
@app.get('/popular_diagnosis_year/{year}')
def get_popular_diagnosis_by_year(year: int):
    with Session(autoflush=False, bind=engine) as db:
        subquery = db.query(func.count(Appointment.id).label('count'), Appointment.diagnosis_id).filter(extract('year', Appointment.date_of_visit) == year).group_by(Appointment.diagnosis_id).subquery()
        diagnosis = db.query(Diagnosis).join(subquery, Diagnosis.id == subquery.c.diagnosis_id).order_by(desc(subquery.c.count)).first()
    return {'diagnosis': diagnosis}

@app.get('/appointments_diagnosis/{diagnosis_id}')
def get_appointments_by_diagnosis(diagnosis_id: int):
    with Session(autoflush=False, bind=engine) as db:
        appointments = db.query(Appointment).filter(Appointment.diagnosis_id == diagnosis_id).all()
        response = []
        for appointment in appointments:
            response.append(appointment)
    return {'appointments': response}
@app.get('/doctor_appointments_week/{week}')
def get_doctor_appointments_by_week(week: int):
    with Session(autoflush=False, bind=engine) as db:
        appointments = db.query(Doctor.id, func.count(Appointment.id)).join(Appointment).filter(extract('week', Appointment.date_of_visit) == week).group_by(Doctor.id).all()
        response = []
        for doctor_id, count in appointments:
            response.append({'doctor_id': doctor_id, 'count': count})
    return {'appointments': response}
@app.get('/discount_appointments')
def get_discount_appointments():
    with Session(autoflush=False, bind=engine) as db:
        appointments = db.query(Appointment).join(Patient).filter(Patient.discount > 0).all()
        response = []
        for appointment in appointments:
            response.append(appointment)
    return {'appointments': response}
@app.get('/efficient_doctor_month/{month}')
def get_efficient_doctor_by_month(month: int):
    with Session(autoflush=False, bind=engine) as db:
        subquery = db.query(func.sum(Appointment.cost).label('total_cost'), Appointment.doctor_id).filter(extract('month', Appointment.date_of_visit) == month).group_by(Appointment.doctor_id).subquery()
        doctor = db.query(Doctor).join(subquery, Doctor.id == subquery.c.doctor_id).order_by(desc(subquery.c.total_cost)).first()
    return {'doctor': doctor}

