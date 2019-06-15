package hyperjson

type ArrayConsumer func() error

func (p *Parser) ReadArray(consumer ArrayConsumer) error {
	if err := p.ConsumeArrayBegin(); err != nil {
		return err
	}

	for {
		next, err := p.NextType()
		if err != nil {
			return err
		}

		// stop at the end of the array
		if next == TypeArrayEnd {
			return p.consumeArrayEnd()
		}

		// call the consumer
		if err := consumer(); err != nil {
			return err
		}
	}
}
