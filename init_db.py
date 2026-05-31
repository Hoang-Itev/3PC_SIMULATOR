import json
import os

def create_synthetic_datasets():
    if not os.path.exists("datasets"):
        os.makedirs("datasets")

    # Dữ liệu khởi tạo cho 3 Node 
    dbs = {
        "datasets/hotel_db.json": {"resource": "Hotel_Rooms", "available": 10, "locked": 0},
        "datasets/flight_db.json": {"resource": "Flight_Seats", "available": 10, "locked": 0},
        "datasets/car_db.json": {"resource": "Car_Rentals", "available": 10, "locked": 0}
    }

    for path, data in dbs.items():
        with open(path, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Đã tạo database vật lý: {path}")

if __name__ == "__main__":
    create_synthetic_datasets()