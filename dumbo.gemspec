# coding: utf-8

Gem::Specification.new do |spec|
  spec.name          = "dumbo"
  spec.version       = "0.1.0"
  spec.authors       = ["remerge GmbH"]
  spec.email         = ["tech@remerge.io"]
  spec.summary       = %q{}
  spec.description   = %q{}
  spec.homepage      = "https://github.com/remerge/dumbo"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]
end
