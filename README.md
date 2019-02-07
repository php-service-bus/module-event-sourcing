[![Build Status](https://travis-ci.org/php-service-bus/module-event-sourcing.svg?branch=v3.0)](https://travis-ci.org/php-service-bus/module-event-sourcing)
[![Code Coverage](https://scrutinizer-ci.com/g/php-service-bus/module-event-sourcing/badges/coverage.png?b=v3.0)](https://scrutinizer-ci.com/g/php-service-bus/module-event-sourcing/?branch=v3.0)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/php-service-bus/module-event-sourcing/badges/quality-score.png?b=v3.0)](https://scrutinizer-ci.com/g/php-service-bus/module-event-sourcing/?branch=v3.0)

Table of contents
* [What is Event Sourcing?](https://github.com/php-service-bus/module-event-sourcing#what-is-event-sourcing)
* [Field of appliance](https://github.com/php-service-bus/module-event-sourcing#field-of-appliance)
* [Event stream](https://github.com/php-service-bus/module-event-sourcing#event-stream)
* [Problems](https://github.com/php-service-bus/module-event-sourcing#problems)
* [Snapshots](https://github.com/php-service-bus/module-event-sourcing#snapshots)
* [Projections](https://github.com/php-service-bus/module-event-sourcing#projections)
* [Indexes](https://github.com/php-service-bus/module-event-sourcing#indexes)
* [Aggregate example](https://github.com/php-service-bus/module-event-sourcing#aggregate-example)
* [Available methods](https://github.com/php-service-bus/module-event-sourcing#available-methods)
* [Life cycle](https://github.com/php-service-bus/module-event-sourcing#life-cycle)
* [Installation](https://github.com/php-service-bus/module-event-sourcing#installation)
* [Working with aggregates](https://github.com/php-service-bus/module-event-sourcing#working-with-aggregates)
* [Working with indexes](https://github.com/php-service-bus/module-event-sourcing#working-with-indexes)
* [Working with snapshots](https://github.com/php-service-bus/module-event-sourcing#working-with-snapshots)

#### What is Event Sourcing?
While the classic scheme implies the storage of the final aggregate state, Event Sourcing persists the state of business entity as a sequence of state-changing events. For example, we have ```User<string:name, string:email, string:password_hash>``` structure. All data is stored in database as a table of ```users``` with fields of the same name. This approach doesn’t let us see the history of user data changes: at what time, by whom and under which conditions they were initiated. Also it isn’t possible to cancel some operations. Event Sourcing ensures that all changes to user state are stored as a sequence of events. For example ```UserRegisteredEvent```, ```UserPasswordChangedEvent```, ```UserEmailChangedEvent```, ```UserPasswordChangedEvent```. To restore current aggregate state we need to apply all events one by one in the initial order. As a result we can have a full history of changes.

For more details look up the [Event Sourcing](https://microservices.io/patterns/data/event-sourcing.html) description

#### Field of appliance
We cannot use Event Sourcing everywhere. It is used when we want to monitor and control all the each change of our business entity.

For example, we have a payment aggregate that includes all payment details. It can be switched to many states; can be cancelled (fully or partially) etc. Looking at this from business perspective, it can be very useful to see the history and be able to influence on it. In such cases Event Sourcing is the best option.

Another case is the list of currencies. Any currency has ISO 4217 codes (digital and letter), which never change. We don’t need to control changes (they won’t happen), that means in that case Event Sourcing is redundant.

#### Event stream
Event Stream is a sorted list of events, which were applied to an aggregate. Any new event increases the stream version by one.

#### Problems
There is a certain amount of problems in this approach. Firstly, it is the redundancy of data (we store a lot of unnormalized data (serialized events)). Secondly, we need to convert the event stream into an aggregate, which requires server resources. Thirdly, we don’t have a possibility to search through the fields (we don’t have them, we only have serialized events representation).

#### Snapshots
Snapshot — type of memorization used to help optimize rebuilding state. If we have to rebuild state from a large stream of events, it can be cumbersome and slow. This is a problem when you want your system to be fast and responsive. We take snapshots of a projection taken at various points in the stream so that we can begin rebuilding state from a snapshot instead of having to replay the entire stream. A snapshot is a cache of a projection of state at some point in time.

#### Projections
Effective work with Event Sourcing requires two interfaces: write model (aggregate) and read model (projection). Projection is a representation of state based on current understanding of what we need. It is generated according to the changes and in the needed form. Essentially this is a cached data structure, which was specially generated for specific request. This allows to exclude the necessity of queries with join completely as the data is saved the in a suitable form.

#### Indexes
To solve a data cleaning problem, you can take any key/value storage for mapping implementation. For example, we need to assure the uniqueness of an email of a user. In that case a classic Event Sourcing implementation is fairly demanding. But another way is possible: when we create a user, we document his identity and email into a special storage. When we create another user, we can check, does anybody else use this email or not. Also due to this approach the problem of search is solved.

#### Aggregate example
```php
final class Customer extends Aggregate
{
    private $name;
    private $email;

    public function register(CustomerId $id,  string $name, string $email): void
    {
        $this->raise(new CustomerRegisteredEvent($id, $name, $email));
    }

    public function rename(string $newName): self
    {
        $this->raise(new CustomerRenamed($this->name, $newName));
    }

    private function onCustomerRegisteredEvent(CustomerRegisteredEvent $event): void
    {
        $this->name  = $event->name;
        $this->email = $event->email;
    }

    private function onCustomerRenamed(CustomerRenamed $event): void
    {
        $this->name = $event->newName;
    }
}
```
#### Available methods
While using the aggregate the following secure methods are available:
* [close()](https://github.com/php-service-bus/event-sourcing/blob/v3.0/src/Aggregate.php#L150): A soft delete analogue. We do not delete the event stream, we just tag it as closed. A closed stream cannot be received, and hence modified
* [raise()](https://github.com/php-service-bus/event-sourcing/blob/v3.0/src/Aggregate.php#L120): Appliance of an event to an aggregate. Naming of event listeners follows an ```on{ClassName}``` pattern, where *on* is a prefix, *{ClassName}* – a basic name of event class.

#### Life cycle
While creating a new aggregate event [AggregateCreated](https://github.com/php-service-bus/event-sourcing/blob/v3.0/src/Contract/AggregateCreated.php) will be sent into transport;  when the aggregate is closed - [AggregateClosed](https://github.com/php-service-bus/event-sourcing/blob/v3.0/src/Contract/AggregateClosed.php)

#### Installation
```bash
composer req php-service-bus/module-event-sourcing
composer req php-service-bus/storage-sql
```

```php
$module = EventSourcingModule::withSqlStorage(DatabaseAdapter::class);
```

Enable module:

```php
$bootstrap->applyModules($module);
```

#### Working with aggregates
[EventSourcingProvider](https://github.com/php-service-bus/module-event-sourcing/blob/v3.0/src/EventSourcingProvider.php) is used to work with aggregates, which has the following methods  (the [Promise](https://github.com/amphp/amp/blob/v3.0/lib/Promise.php) object is returned in each of them):
* [load()](https://github.com/php-service-bus/module-event-sourcing/blob/v3.0/src/EventSourcingProvider.php#L63): Load aggregate from database
* [save()](https://github.com/php-service-bus/module-event-sourcing/blob/v3.0/src/EventSourcingProvider.php#L106): Save a new aggregate
* [revert()](https://github.com/php-service-bus/module-event-sourcing/blob/v3.0/src/EventSourcingProvider.php#L175): Revert aggregate to specified version

#### Working with indexes
To work with indexes, use [IndexProvider](https://github.com/php-service-bus/module-event-sourcing/blob/v3.0/src/IndexProvider.php), which has the following methods  (the [Promise](https://github.com/amphp/amp/blob/v3.0/lib/Promise.php) object is returned in each of them):
* [get()](https://github.com/php-service-bus/module-event-sourcing/blob/v3.0/src/IndexProvider.php#L53): Receive a saved value
* [has()](https://github.com/php-service-bus/module-event-sourcing/blob/v3.0/src/IndexProvider.php#L90): Was a value saved
* [add()](https://github.com/php-service-bus/module-event-sourcing/blob/v3.0/src/IndexProvider.php#L128): Add a value to index. Working principle is similar to the ```\Memcached:add```: method, if values with such a keyword didn’t exist it will return «true», otherwise - «false» (best choice)
* [remove()](https://github.com/php-service-bus/module-event-sourcing/blob/v3.0/src/IndexProvider.php#L168): Delete saved value
* [update()](https://github.com/php-service-bus/module-event-sourcing/blob/v3.0/src/IndexProvider.php#L200): Update saved value

#### Working with snapshots
By default only one strategy of snapshots generation is implemented - [SnapshotVersionTrigger](https://github.com/php-service-bus/event-sourcing/blob/v3.0/src/Snapshots/Triggers/SnapshotVersionTrigger.php), which is based on versions (generates a snapshot every *N* version changes). Snapshots are created automatically, with an exclusion of strategy selection (you can implement your own snapshots with the help of  [SnapshotTrigger](https://github.com/php-service-bus/event-sourcing/blob/v3.0/src/Snapshots/Triggers/SnapshotTrigger.php) interface) no setting is required
