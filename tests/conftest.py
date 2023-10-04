import os
import random
import string
import typing

import pandas
import toml


def rand_str(length: int = 5) -> str:
    _letters = list(string.ascii_letters)
    return "".join(random.choice(_letters) for _ in range(length))


def fake_csv(output_dir: str, file_name: str | None = None) -> str:
    _file_name: str = file_name or os.path.join(output_dir, rand_str() + ".csv")
    pandas.DataFrame({rand_str(): [random.randint(0, 100) for _ in range(100)]}).to_csv(
        _file_name
    )
    return _file_name


def fake_feather(output_dir: str, file_name: str | None = None) -> str:
    _file_name: str = file_name or os.path.join(output_dir, rand_str() + ".ft")
    pandas.DataFrame(
        {rand_str(): [random.randint(0, 100) for _ in range(100)]}
    ).to_feather(_file_name)
    return _file_name


def fake_toml(output_dir: str, file_name: str | None = None) -> str:
    _file_name: str = file_name or os.path.join(output_dir, rand_str()) + ".toml"
    toml.dump(
        {rand_str(): [random.randint(0, 100) for _ in range(100)]},
        open(_file_name, "w"),
    )
    return _file_name


def fake_nml(output_dir: str, file_name: str | None = None) -> str:
    _file_name: str = file_name or os.path.join(output_dir, rand_str()) + ".nml"
    with open(_file_name, "w") as out_f:
        out_str = ["&" + rand_str().upper()]
        for _ in range(0, random.randint(4, 10)):
            _key = f"{random.choice(list(string.ascii_uppercase))}{random.choice(['', '%'+rand_str().upper()])}{random.choice(['', '('+str(random.randint(1, 10))+')'])}"
            if ")" not in _key and "%" not in _key:
                _val = random.choice(
                    [
                        random.randint(0, 100),
                        f"'{rand_str().upper()}'",
                        random.random() * random.randint(1, 100),
                    ]
                )
            else:
                _val = random.randint(0, 100)
            out_str.append(f"{_key}={_val}")
        out_str.append("/")
        out_f.write("\n".join(out_str))
    return _file_name


def to_nml(dictionary: typing.Dict[str, typing.Any], file_name: str) -> None:
    with open(file_name, "w") as out_f:
        out_str = ["&DEMONML"]
        for key, value in dictionary.items():
            _value = value if not isinstance(value, str) else "'" + value + "'"
            out_str.append(f"{key.upper()}={_value}")
        out_str.append("/")
        out_f.write("\n".join(out_str))
