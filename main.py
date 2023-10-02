import psycopg2


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients(
            client_id SERIAL PRIMARY KEY,
            name VARCHAR(20) NOT NULL,
            lastname VARCHAR(20) NOT NULL,
            email VARCHAR(30) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS phone_numbers(
            client_id INTEGER NOT NULL REFERENCES clients(client_id),
            phone INTEGER UNIQUE
        );
        """)

        conn.commit()


def add_client(conn, name, lastname, email, phone=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO clients(name, lastname, email)
        VALUES (%s, %s, %s) RETURNING client_id;
        """, (name, lastname, email))

        client_id = cur.fetchone()

        if phone != None:
            cur.execute("""
            INSERT INTO phone_numbers
            VALUES (%s,%s);
            """, (client_id, phone))

            conn.commit()


def add_number(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO phone_numbers
        VALUES (%s, %s)
        """, (client_id, phone))

        conn.commit()


def change_client(conn, client_id, name=None, lastname=None, email=None, phones=None):
    with conn.cursor() as cur:
        if name != None:
            cur.execute("""
            UPDATE clients
            SET name=%s
            WHERE client_id=%s
            """, (name, client_id))

            conn.commit()

        if lastname != None:
            cur.execute("""
            UPDATE clients
            SET lastname=%s
            WHERE client_id=%s
            """, (lastname, client_id))

            conn.commit()

        if email != None:
            cur.execute("""
            UPDATE clients
            SET email=%s
            WHERE client_id=%s
            """, (email, client_id))

            conn.commit()

        if phones != None:
            cur.execute("""
            SELECT client_id FROM phone_numbers
            WHERE client_id=%s;
            """, (client_id,))

            test = cur.fetchone()

            if test == None:
                for phone in phones:
                    cur.execute("""
                        INSERT INTO phone_numbers
                        VALUES (%s, %s);
                        """, (client_id, phone))

                    conn.commit()
            else:
                cur.execute("""
                DELETE FROM phone_numbers
                WHERE client_id=%s;
                """, (client_id,))

                conn.commit()

                for phone in phones:
                    cur.execute("""
                    INSERT INTO phone_numbers
                    VALUES (%s, %s);
                    """, (client_id, phone))

                conn.commit()


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone_numbers
        WHERE client_id = %s AND phone=%s;
        """, (client_id, phone))

        conn.commit()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
                    SELECT client_id FROM phone_numbers
                    WHERE client_id=%s;
                    """, (client_id,))

        test = cur.fetchone()

        if test != None:
            cur.execute("""
            DELETE FROM phone_numbers
            WHERE id=%s;
            """, (client_id,))

            conn.commit()

        cur.execute("""
        DELETE FROM clients
        WHERE client_id = %s;
        """, (client_id,))

        conn.commit()


def find_client(conn, name=None, lastname=None, email=None, phone=None):
    with conn.cursor() as cur:
        if name != None:
            cur.execute("""
            SELECT c.client_id, name, lastname, email, phone FROM clients c
            LEFT JOIN phone_numbers pn ON c.client_id = pn.client_id
            WHERE name=%s
            """, (name,))

            print(cur.fetchone())

        if lastname != None:
            cur.execute("""
            SELECT c.client_id, name, lastname, email, phone FROM clients c
            LEFT JOIN phone_numbers pn ON c.client_id = pn.client_id
            WHERE lastname=%s
            """, (lastname,))

            print(cur.fetchone())

        if email != None:
            cur.execute("""
            SELECT c.client_id, name, lastname, email, phone FROM clients c
            LEFT JOIN phone_numbers pn ON c.client_id = pn.client_id
            WHERE email=%s
            """, (email,))

            print(cur.fetchone())

        if phone != None:
            cur.execute("""
            SELECT c.client_id, name, lastname, email, phone FROM clients c
            LEFT JOIN phone_numbers pn ON c.client_id = pn.client_id
            WHERE phone=%s
            """, (phone,))

            print(cur.fetchone())


with psycopg2.connect(database="Clients", user="postgres", password="4815162342.cth84.te") as conn:

    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE phone_numbers;
        DROP TABLE clients;
        """)

    create_db(conn)
    add_client(conn, 'alex', 'ford', '1@1.ru', 1111)
    add_client(conn, 'john', 'gold', '2@1.ru', 2222)
    add_client(conn, 'sam', 'parks', '3@1.ru')

    print('Созданые и заполненые таблицы:')
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM clients
        """)

        print(cur.fetchall())
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM phone_numbers
        """)

        print(cur.fetchall())

    add_number(conn, 3, 3333)

    print()
    print('Добавление телефона:')
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM phone_numbers
        """)

        print(cur.fetchall())

    change_client(conn,3, phones=[5555])
    change_client(conn, 3, lastname='reed')

    print()
    print('Изменение данных о клиенте:')
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM phone_numbers
        """)

        print(cur.fetchall())

    delete_phone(conn, 3, 5555)

    print()
    print('Удаление телефона:')
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM phone_numbers
        """)

        print(cur.fetchall())

    delete_client(conn, 3)

    print()
    print('Удаление клиента из базы:')
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM phone_numbers
        """)

        print(cur.fetchall())

    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM clients
        """)

        print(cur.fetchall())

    print()
    print('Поиск клиента:')
    find_client(conn, phone='2222')
    find_client(conn, 'alex')