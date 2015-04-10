# Python DataStreams

A streaming library to make your data processing beautiful and succinct.

```python
>>> from datastreams import DataStream

>>> DataStream("Hello, gorgeous.")\
...     .filter(lambda char: char.isalpha())\
...     .map_method('lower')\
...     .count_frequency().to_list()
[('e', 2), ('g', 2), ('h', 1), ('l', 2), ('o', 3), ('s', 1), ('r', 1), ('u', 1)]
```

## Why

Inspiration for this library came when drudging through ETL/feature engineering tasks.  The data being processed was large, and requiring a lot of cleanup/coercion, and making it reliable and performant usually meant sacrificing code clarity and brevity.
  
These tasks, and many others, can be better modeled as a series of filters, transforms, and reductions, making them clearer and easier to optimize.  DataStreams is the implementation of these ideals, using method chaining and practical functional elements to create elegant, legible, and succinct data pipelines.

DataStreams:

- Are fast
- Are succinct
- Have sane syntax
- Are just Python
  
  
## Pipelining With DataStreams

DataStreams are perfect for pipelined feature calculation:

```python
def calc_name(user):
    user.first_name = user.name.split(' ')[0] if user.name else ''
    return user
    
def calc_age(user):
   user.age = datetime.now() - user.birthday
   return user

DataStream(users)\
    .map(calc_name)\
    .map(calc_age)\
    .for_each(User.save)\
    .execute() # <- Nothing happens till here
```

Theses calculations are efficient because streams are generators by default, and no memory is wasted on unnecessary intermediary collections.

This is even better when certain calculation steps are long, and must be wrapped in their own functions.  Of course, for brevity, you can use `set`:

```python
DataStream(users)\
    .set('first_name', lambda user: user.name.split(' ')[0] if user.name else '')\
    .set('age', lambda user: datetime.now() - user.birthday)\
    .for_each(User.save)\
    .execute()
```

## Joins

You can join DataStreams - even streams of objects!

```python
userstream = DataStream(users)
transactstream = DataStream(transactions)

user_spend = userstream.join('inner', 'user_id', transactstream)\
    .group_by('user_id')\
    .map(lambda usertrans: (usertrans[0], sum(tran.price for tran in usertrans[1])))\
    .to_dict()
    
# {'47328129': 240.95, '48190234': 40.73, ...} 
```


## `where` Clause

Chained `filter`s are a bit tiresome. `where` lets you perform simple filtering using more accessible language:
  
```python
DataStream(users)\
    .where('age').gteq(18)\
    .where('age').lt(35)\
    .where('segment').is_in(target_segments)\
    .for_each(do_something)
```

Instead of:

```python
DataStream(users)\
    .filter(lambda user: user.age >= 18)\
    .filter(lambda user: user.age < 35)\
    .filter(lambda user: user.segment in target_segments)\
    .for_each(do_something)
```

I bet you got tired just _reading_ that many lambdas!

# Influences

- Javascript
- Spark
- Java streaming lib
- Pandas
- LINQ

