from tokenizers import Tokenizer
from tokenizers.models import BPE
from tokenizers.trainers import BpeTrainer
from tokenizers.pre_tokenizers import Whitespace


def tokenize_with_bpe(text, bpe_pairs):
    """
    Tokenize a text using the Byte Pair Encoding (BPE) algorithm.

    Args:
        text (str): The text to tokenize.
        bpe_pairs (dict): The dictionary of BPE merge pairs.

    Raises:
        ValueError: If BPE pairs are not provided.

    Returns:
        list: The list of tokens after BPE tokenization.
    """
    if not bpe_pairs:
        raise ValueError("BPE pairs are not provided.")

    tokens = text.split()
    is_pair_merged = True
    while is_pair_merged:
        is_pair_merged = False
        for i in range(len(tokens) - 1):
            pair = (tokens[i], tokens[i + 1])
            if pair in bpe_pairs:
                merged_token = ''.join(pair)
                tokens = tokens[:i] + [merged_token] + tokens[i + 2:]
                is_pair_merged = True
                break

    return tokens


def tokenize_input_file(input_file_path, bpe_pairs):
    """
    Tokenize the contents of an input text file using BPE.

    Args:
        input_file_path (str): The path to the input file.
        bpe_pairs (dict): The dictionary of BPE merge pairs.

    Returns:
        list: The list of tokens after BPE tokenization of the file's content.
    """
    with open(input_file_path, 'r', encoding='utf-8') as file:
        text = file.read()
    return tokenize_with_bpe(text, bpe_pairs)



def learn_bpe_tokenizer(input_file_path, output_model_path, vocab_size=10000):
    """
    Learn a BPE tokenizer from a given text file and save the model.

    Args:
        input_file_path (str): The path to the input text file.
        output_model_path (str): The path where the BPE model should be saved.
        vocab_size (int, optional): The size of the vocabulary. Defaults to 10000.

    """
    
    tokenizer = Tokenizer(BPE())
    tokenizer.pre_tokenizer = Whitespace()
    trainer = BpeTrainer(vocab_size=vocab_size, min_frequency=2)
    lines = []
    with open(input_file_path, 'r', encoding='utf-8') as file:
        for line in file:
            lines.append(line.strip())
    tokenizer.train_from_iterator(lines, trainer)
    tokenizer.save(output_model_path)
    print(f"BPE tokenizer model saved to {output_model_path}")



# Example use tokenize_input_file to tokenize the input file
# tokens = tokenize_input_file(INPUT_FILE, BPE_PAIRS)

# Example use learn_bpe_tokenizer to learn a BPE tokenizer model
INPUT_FILE = 'cleaned_text.txt'
OUTPUT_MODEL = 'bpe_tokenizer.json'
VOCAB_SIZE = 10000

# learn_bpe_tokenizer(INPUT_FILE, OUTPUT_MODEL, VOCAB_SIZE)