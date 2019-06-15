package hyperjson

import "github.com/pkg/errors"

func (p *Parser) Skip() error {
	next, err := p.NextType()
	if err != nil {
		return err
	}

	return p.skip(next)
}

func (p *Parser) skip(next Type) error {
	var err error

	switch next {
	case TypeString:
		_, err = p.readString()

	case TypeNumber:
		_, err = p.readNumber()

	case TypeBoolean:
		_, err = p.readBoolean()

	case TypeNull:
		_, err = p.readNull()

	case TypeObjectBegin:
		err = p.skipObject()

	case TypeArrayBegin:
		err = p.skipArray()

	default:
		err = errors.Errorf("can not skip unexpected symbol of type '%s'", next)
	}
	return err
}

func (p *Parser) skipArray() error {
	if err := p.consumeArrayBegin(); err != nil {
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
		if err := p.skip(next); err != nil {
			return err
		}
	}
}

func (p *Parser) skipObject() error {
	if err := p.consumeObjectBegin(); err != nil {
		return err
	}

	for {
		next, err := p.NextType()
		if err != nil {
			return err
		}

		// if there is no key, object must be ending
		if next == TypeObjectEnd {
			return p.consumeObjectEnd()
		}

		// read the key
		if _, err := p.readString(); err != nil {
			return err
		}

		if err := p.Skip(); err != nil {
			return err
		}
	}
}
