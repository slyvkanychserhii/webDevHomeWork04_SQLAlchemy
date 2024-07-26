
from contextlib import contextmanager

from sqlalchemy import (
    create_engine,
    ForeignKey,
    Column,
    Integer,
    String,
    Numeric,
    Boolean,
    func
)
from sqlalchemy.orm import (
    sessionmaker,
    declarative_base,
    relationship,
    aliased
)

engine = create_engine("sqlite:///foo.db", echo=False)

# sqlite3
# sqlite> .help
# sqlite> .open foo.db
# sqlite> .tables
# sqlite> .mode column
# sqlite> .headers on
# sqlite> select * from products;
# sqlite> select * from categories;

Base = declarative_base()


class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    price = Column(Numeric(10, 2))
    in_stock = Column(Boolean, default=True)
    category_id = Column(Integer, ForeignKey("categories.id"))
    category = relationship("Category", back_populates="products")

    def __repr__(self):
        return "<Product(name='%s', price='%s', in_stock='%s')>" % (
            self.name,
            self.price,
            self.in_stock,
        )


class Category(Base):
    __tablename__ = "categories"
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    description = Column(String(255))
    products = relationship("Product", order_by=Product.id, back_populates="category")

    def __repr__(self):
        return "<Category(name='%s', description='%s')>" % (
            self.name,
            self.description,
        )


Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)


session_factory = sessionmaker(bind=engine)


@contextmanager
def session_scope():
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")
        raise
    finally:
        session.close()


# Задача 1: Наполнение данными
# Добавьте в базу данных следующие категории и продукты
# Добавление категорий: Добавьте в таблицу categories следующие категории:
# Название: "Электроника", Описание: "Гаджеты и устройства."
# Название: "Книги", Описание: "Печатные книги и электронные книги."
# Название: "Одежда", Описание: "Одежда для мужчин и женщин."
#
# Добавление продуктов: Добавьте в таблицу products следующие продукты, убедившись, что каждый продукт
# связан с соответствующей категорией:
# Название: "Смартфон", Цена: 299.99, Наличие на складе: True, Категория: Электроника
# Название: "Ноутбук", Цена: 499.99, Наличие на складе: True, Категория: Электроника
# Название: "Научно-фантастический роман", Цена: 15.99, Наличие на складе: True, Категория: Книги
# Название: "Джинсы", Цена: 40.50, Наличие на складе: True, Категория: Одежда
# Название: "Футболка", Цена: 20.00, Наличие на складе: True, Категория: Одежда

categories = [
    Category(name="Электроника", description="Гаджеты и устройства.", products=[
        Product(name="Смартфон", price=299.99, in_stock=True),
        Product(name="Ноутбук", price=499.99, in_stock=True)
    ]),
    Category(name="Книги", description="Печатные книги и электронные книги.", products=[
        Product(name="Научно-фантастический роман", price=15.99, in_stock=True)
    ]),
    Category(name="Одежда", description="Одежда для мужчин и женщин.", products=[
        Product(name="Джинсы", price=40.50, in_stock=True),
        Product(name="Футболка", price=20.00, in_stock=True)
    ])
]

with session_scope() as session:
    session.add_all(categories)


# Задача 2: Чтение данных
# Извлеките все записи из таблицы categories. Для каждой категории извлеките и выведите все связанные с ней
# продукты, включая их названия и цены.

def get_all_categories_with_products():
    print("\nЗадача 2: Чтение данных")
    with session_scope() as session:
        query = session.query(Category)
        print("raw query:", query, sep="\n")
        print("result:")
        categories = query.all()
        for category in categories:
            print(category)
            for product in category.products:
                print("\t", product)

get_all_categories_with_products()

# Задача 3: Обновление данных
# Найдите в таблице products первый продукт с названием "Смартфон". Замените цену этого продукта на 349.99.

def update_product_price_by_name(name, price):
    with session_scope() as session:
        query = session.query(Product).filter(Product.name == name)
        # print("raw query:", query, sep="\n")
        product = query.first()
        if product:
            product.price = price

update_product_price_by_name("Смартфон", 349.99)

# Задача 4: Агрегация и группировка
# Используя агрегирующие функции и группировку, подсчитайте общее количество продуктов в каждой категории.

def get_count_of_products_by_categories():
    print("\nЗадача 4: Агрегация и группировка")
    with session_scope() as session:
        t1 = aliased(Category, name="t1")
        t2 = aliased(Product, name="t2")
        query = session.query(t1.name, func.count().label("count")).outerjoin(t2).group_by(t1.name) \
            .order_by(func.count().desc())
        print("raw query:", query, sep="\n")
        print("result:")
        for i in query.all():
            print(f"{i.name}: {i.count}")

get_count_of_products_by_categories()

# Задача 5: Группировка с фильтрацией
# Отфильтруйте и выведите только те категории, в которых более одного продукта.

def get_count_of_products_by_categories_gt_one():
    print("\nЗадача 5: Группировка с фильтрацией")
    with session_scope() as session:
        t1 = aliased(Category, name="t1")
        t2 = aliased(Product, name="t2")
        query = session.query(t1.name, func.count().label("count")).outerjoin(t2).group_by(t1.name) \
            .having(func.count() > 1).order_by(func.count().desc())
        print("raw query:", query, sep="\n")
        print("result:")
        for i in query.all():
            print(f"{i.name}: {i.count}")

get_count_of_products_by_categories_gt_one()
