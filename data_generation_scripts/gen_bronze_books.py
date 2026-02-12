"""Generate bronze books CSV test data with a mix of valid and invalid rows.

Valid rows pass all data quality checks from the week 5 silver.books MERGE:
  - isbn IS NOT NULL and non-empty after TRIM
  - title IS NOT NULL and non-empty after TRIM
  - isbn matches ISBN-13 format: 978-X-XX-XXXXXX-X

Invalid rows violate at least one of these checks.

Usage:
    python gen_bronze_books.py --valid <N> --invalid <N> [-o OUTPUT]
"""

import argparse
import csv
import random
import string

# 100 real books: (title, author, genre)
# Source: Goodreads "Best Books Ever" list
BOOKS = [
    ("The Hunger Games", "Suzanne Collins", "Science Fiction"),
    ("Pride and Prejudice", "Jane Austen", "Romance"),
    ("To Kill a Mockingbird", "Harper Lee", "Fiction"),
    ("Harry Potter and the Order of the Phoenix", "J.K. Rowling", "Fantasy"),
    ("The Book Thief", "Markus Zusak", "Fiction"),
    ("Animal Farm", "George Orwell", "Fiction"),
    ("The Chronicles of Narnia", "C.S. Lewis", "Fantasy"),
    ("The Fault in Our Stars", "John Green", "Fiction"),
    ("The Picture of Dorian Gray", "Oscar Wilde", "Fiction"),
    ("The Lightning Thief", "Rick Riordan", "Fantasy"),
    ("Wuthering Heights", "Emily Bronte", "Romance"),
    ("The Giving Tree", "Shel Silverstein", "Fiction"),
    ("The Perks of Being a Wallflower", "Stephen Chbosky", "Fiction"),
    ("Gone with the Wind", "Margaret Mitchell", "Romance"),
    ("The Little Prince", "Antoine de Saint-Exupery", "Fiction"),
    ("Jane Eyre", "Charlotte Bronte", "Romance"),
    ("The Great Gatsby", "F. Scott Fitzgerald", "Fiction"),
    ("Crime and Punishment", "Fyodor Dostoevsky", "Fiction"),
    ("The Da Vinci Code", "Dan Brown", "Thriller"),
    ("Alice's Adventures in Wonderland", "Lewis Carroll", "Fantasy"),
    ("Divergent", "Veronica Roth", "Science Fiction"),
    ("Les Miserables", "Victor Hugo", "Fiction"),
    ("Memoirs of a Geisha", "Arthur Golden", "Fiction"),
    ("Anne of Green Gables", "L.M. Montgomery", "Fiction"),
    ("The Alchemist", "Paulo Coelho", "Fiction"),
    ("Lord of the Flies", "William Golding", "Fiction"),
    ("Brave New World", "Aldous Huxley", "Science Fiction"),
    ("The Hitchhiker's Guide to the Galaxy", "Douglas Adams", "Science Fiction"),
    ("City of Bones", "Cassandra Clare", "Fantasy"),
    ("The Help", "Kathryn Stockett", "Fiction"),
    ("Dracula", "Bram Stoker", "Fiction"),
    ("Fahrenheit 451", "Ray Bradbury", "Science Fiction"),
    ("Charlotte's Web", "E.B. White", "Fiction"),
    ("1984", "George Orwell", "Science Fiction"),
    ("Of Mice and Men", "John Steinbeck", "Fiction"),
    ("Ender's Game", "Orson Scott Card", "Science Fiction"),
    ("The Catcher in the Rye", "J.D. Salinger", "Fiction"),
    ("Little Women", "Louisa May Alcott", "Fiction"),
    ("One Hundred Years of Solitude", "Gabriel Garcia Marquez", "Fiction"),
    ("A Thousand Splendid Suns", "Khaled Hosseini", "Fiction"),
    ("The Outsiders", "S.E. Hinton", "Fiction"),
    ("The Secret Garden", "Frances Hodgson Burnett", "Fiction"),
    ("The Princess Bride", "William Goldman", "Fantasy"),
    ("A Game of Thrones", "George R.R. Martin", "Fantasy"),
    ("The Time Traveler's Wife", "Audrey Niffenegger", "Romance"),
    ("Harry Potter and the Deathly Hallows", "J.K. Rowling", "Fantasy"),
    ("The Odyssey", "Homer", "Fiction"),
    ("Frankenstein", "Mary Shelley", "Science Fiction"),
    ("The Handmaid's Tale", "Margaret Atwood", "Science Fiction"),
    ("A Wrinkle in Time", "Madeleine L'Engle", "Science Fiction"),
    ("The Kite Runner", "Khaled Hosseini", "Fiction"),
    ("The Giver", "Lois Lowry", "Science Fiction"),
    ("Harry Potter and the Prisoner of Azkaban", "J.K. Rowling", "Fantasy"),
    ("The Girl with the Dragon Tattoo", "Stieg Larsson", "Thriller"),
    ("Dune", "Frank Herbert", "Science Fiction"),
    ("Where the Wild Things Are", "Maurice Sendak", "Fiction"),
    ("The Lovely Bones", "Alice Sebold", "Fiction"),
    ("The Adventures of Huckleberry Finn", "Mark Twain", "Fiction"),
    ("Life of Pi", "Yann Martel", "Fiction"),
    ("Lolita", "Vladimir Nabokov", "Fiction"),
    ("A Tale of Two Cities", "Charles Dickens", "Fiction"),
    ("Slaughterhouse-Five", "Kurt Vonnegut", "Science Fiction"),
    ("The Bell Jar", "Sylvia Plath", "Fiction"),
    ("Matilda", "Roald Dahl", "Fiction"),
    ("Water for Elephants", "Sara Gruen", "Fiction"),
    ("Harry Potter and the Sorcerer's Stone", "J.K. Rowling", "Fantasy"),
    ("The Stand", "Stephen King", "Thriller"),
    ("Catch-22", "Joseph Heller", "Fiction"),
    ("The Adventures of Sherlock Holmes", "Arthur Conan Doyle", "Mystery"),
    ("The Pillars of the Earth", "Ken Follett", "Fiction"),
    ("Rebecca", "Daphne du Maurier", "Mystery"),
    ("Watership Down", "Richard Adams", "Fantasy"),
    ("The Color Purple", "Alice Walker", "Fiction"),
    ("Great Expectations", "Charles Dickens", "Fiction"),
    ("Outlander", "Diana Gabaldon", "Romance"),
    ("Anna Karenina", "Leo Tolstoy", "Fiction"),
    ("The Fellowship of the Ring", "J.R.R. Tolkien", "Fantasy"),
    ("A Clockwork Orange", "Anthony Burgess", "Science Fiction"),
    ("One Flew Over the Cuckoo's Nest", "Ken Kesey", "Fiction"),
    ("The Brothers Karamazov", "Fyodor Dostoevsky", "Fiction"),
    ("My Sister's Keeper", "Jodi Picoult", "Fiction"),
    ("A Tree Grows in Brooklyn", "Betty Smith", "Fiction"),
    ("The Road", "Cormac McCarthy", "Science Fiction"),
    ("The Golden Compass", "Philip Pullman", "Fantasy"),
    ("Harry Potter and the Goblet of Fire", "J.K. Rowling", "Fantasy"),
    ("Siddhartha", "Hermann Hesse", "Fiction"),
    ("And Then There Were None", "Agatha Christie", "Mystery"),
    ("Don Quixote", "Miguel de Cervantes", "Fiction"),
    ("Angela's Ashes", "Frank McCourt", "Biography"),
    ("The Old Man and the Sea", "Ernest Hemingway", "Fiction"),
    ("The Poisonwood Bible", "Barbara Kingsolver", "Fiction"),
    ("Beloved", "Toni Morrison", "Fiction"),
    ("The Count of Monte Cristo", "Alexandre Dumas", "Fiction"),
    ("The Shining", "Stephen King", "Thriller"),
    ("Moby-Dick", "Herman Melville", "Fiction"),
    ("The Grapes of Wrath", "John Steinbeck", "Fiction"),
    ("War and Peace", "Leo Tolstoy", "Fiction"),
    ("The Hobbit", "J.R.R. Tolkien", "Fantasy"),
    ("Treasure Island", "Robert Louis Stevenson", "Fiction"),
    ("The Jungle Book", "Rudyard Kipling", "Fiction"),
]


def random_isbn():
    """Generate a valid ISBN-13 string matching 978-X-XX-XXXXXX-X."""
    d = lambda n: "".join(random.choices(string.digits, k=n))
    return f"978-{d(1)}-{d(2)}-{d(6)}-{d(1)}"


def random_book():
    """Pick a random (title, author, genre) from the real books list."""
    return random.choice(BOOKS)


def gen_valid_row(used_isbns):
    """Generate a row that passes all silver data quality checks."""
    while True:
        isbn = random_isbn()
        if isbn not in used_isbns:
            used_isbns.add(isbn)
            break
    title, author, genre = random_book()
    return {
        "isbn": isbn,
        "title": title,
        "author": author,
        "genre": genre,
    }


def gen_invalid_row(used_isbns):
    """Generate a row that fails at least one silver data quality check.

    Cycles through failure modes so every type of defect is represented:
      0 - null isbn
      1 - empty isbn
      2 - isbn fails regex (wrong format)
      3 - null title
      4 - empty title
      5 - whitespace-only isbn (empty after TRIM)
      6 - whitespace-only title (empty after TRIM)
    """
    mode = gen_invalid_row.counter % 7
    gen_invalid_row.counter += 1

    title, author, genre = random_book()
    base = {
        "isbn": random_isbn(),
        "title": title,
        "author": author,
        "genre": genre,
    }

    if mode == 0:
        base["isbn"] = ""
    elif mode == 1:
        base["isbn"] = ""
    elif mode == 2:
        bad_formats = [
            "1234567890123",             # no dashes
            "979-0-12-345678-9",         # starts with 979 not 978
            "978-12-34-567890-1",        # wrong group lengths
            "978-X-YZ-ABCDEF-G",         # letters instead of digits
            f"978-{random.randint(0,9)}", # truncated
        ]
        base["isbn"] = random.choice(bad_formats)
    elif mode == 3:
        base["title"] = ""
    elif mode == 4:
        base["title"] = ""
    elif mode == 5:
        base["isbn"] = "   "
    elif mode == 6:
        base["title"] = "   "

    # Make sure the isbn (if non-empty and valid-looking) doesn't collide
    if base["isbn"] and base["isbn"].strip():
        used_isbns.add(base["isbn"])

    return base


gen_invalid_row.counter = 0


def main():
    parser = argparse.ArgumentParser(
        description="Generate bronze books CSV with valid and invalid rows."
    )
    parser.add_argument("--valid", type=int, required=True, help="Number of valid rows")
    parser.add_argument("--invalid", type=int, required=True, help="Number of invalid rows")
    parser.add_argument(
        "-o", "--output", default="bronze_books.csv",
        help="Output CSV file path (default: bronze_books.csv)",
    )
    args = parser.parse_args()

    if args.valid < 0 or args.invalid < 0:
        parser.error("Counts must be non-negative")

    random.seed(args.valid * 100003 + args.invalid)

    used_isbns = set()
    rows = []

    for _ in range(args.valid):
        rows.append(gen_valid_row(used_isbns))

    for _ in range(args.invalid):
        rows.append(gen_invalid_row(used_isbns))

    random.shuffle(rows)

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["isbn", "title", "author", "genre"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows ({args.valid} valid, "
          f"{args.invalid} invalid) to {args.output}")


if __name__ == "__main__":
    main()
