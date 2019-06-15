package hyperjson

// The key parameter must be used before calling any other method on the parser
type ObjectConsumer func(key []byte) error

func (p *Parser) ReadObject(consumer ObjectConsumer) error {
	if err := p.ConsumeObjectBegin(); err != nil {
		return err
	}

	for {
		next, err := p.NextType()
		if err != nil {
			return err
		}

		// if there is no key, object must be ending
		if next != TypeString {
			return p.consumeObjectEnd()
		}

		// read the key
		keyToken, err := p.readString()
		if err != nil {
			return err
		}

		// call the consumer
		if err := consumer(keyToken.Value); err != nil {
			return err
		}
	}
}
