requires 'parent', 0;
requires 'curry', 0;
requires 'Future', '>= 0.38';
# requires 'Database::Async', 0;
requires 'URI::postgresql', 0;

requires 'Protocol::PostgreSQL', 0;

on 'test' => sub {
	requires 'Test::More', '>= 0.98';
	requires 'Test::Fatal', '>= 0.010';
	requires 'Test::Refcount', '>= 0.07';
	requires 'Test::PostgreSQL', '>= 1.26';
};

on 'develop' => sub {
	requires 'Test::CPANfile', '>= 0.02';
};
